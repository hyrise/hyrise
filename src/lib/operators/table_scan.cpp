#include "table_scan.hpp"

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "constant_mappings.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/proxy_chunk.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "table_scan/column_between_table_scan_impl.hpp"
#include "table_scan/column_is_null_table_scan_impl.hpp"
#include "table_scan/column_like_table_scan_impl.hpp"
#include "table_scan/column_vs_column_table_scan_impl.hpp"
#include "table_scan/column_vs_value_table_scan_impl.hpp"
#include "table_scan/expression_evaluator_table_scan_impl.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in,
                     const std::shared_ptr<AbstractExpression>& predicate)
    : AbstractReadOnlyOperator{OperatorType::TableScan, in}, _predicate(predicate) {}

void TableScan::set_excluded_chunk_ids(const std::vector<ChunkID>& chunk_ids) { _excluded_chunk_ids = chunk_ids; }

const std::shared_ptr<AbstractExpression>& TableScan::predicate() const { return _predicate; }

const std::string TableScan::name() const { return "TableScan"; }

const std::string TableScan::description(DescriptionMode description_mode) const {
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";

  std::stringstream stream;

  stream << name() << separator;
  stream << "Impl: " << _impl_description;
  stream << separator << _predicate->as_column_name();

  return stream.str();
}

void TableScan::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  expressions_set_transaction_context({_predicate}, transaction_context);
}

void TableScan::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  expression_set_parameters(_predicate, parameters);
}

std::shared_ptr<AbstractOperator> TableScan::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<TableScan>(copied_input_left, _predicate->deep_copy());
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto in_table = input_table_left();

  const auto output_table = std::make_shared<Table>(in_table->column_definitions(), TableType::References);

  _impl = create_impl();
  _impl_description = _impl->description();

  std::mutex output_mutex;

  const auto excluded_chunk_set = std::unordered_set<ChunkID>{_excluded_chunk_ids.cbegin(), _excluded_chunk_ids.cend()};

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(in_table->chunk_count() - excluded_chunk_set.size());

  for (ChunkID chunk_id{0u}; chunk_id < in_table->chunk_count(); ++chunk_id) {
    if (excluded_chunk_set.count(chunk_id)) continue;

    auto job_task = std::make_shared<JobTask>([=, &output_mutex]() {
      const auto chunk_guard = in_table->get_chunk_with_access_counting(chunk_id);
      // The actual scan happens in the sub classes of BaseTableScanImpl
      const auto matches_out = _impl->scan_chunk(chunk_id);
      if (matches_out->empty()) return;

      // The ChunkAccessCounter is reused to track accesses of the output chunk. Accesses of derived chunks are counted
      // towards the original chunk.
      Segments out_segments;

      /**
       * matches_out contains a list of row IDs into this chunk. If this is not a reference table, we can
       * directly use the matches to construct the reference segments of the output. If it is a reference segment,
       * we need to resolve the row IDs so that they reference the physical data segments (value, dictionary) instead,
       * since we don’t allow multi-level referencing. To save time and space, we want to share position lists
       * between segments as much as possible. Position lists can be shared between two segments iff
       * (a) they point to the same table and
       * (b) the reference segments of the input table point to the same positions in the same order
       *     (i.e. they share their position list).
       */
      if (in_table->type() == TableType::References) {
        const auto chunk_in = in_table->get_chunk(chunk_id);

        auto filtered_pos_lists = std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>>{};

        for (ColumnID column_id{0u}; column_id < in_table->column_count(); ++column_id) {
          auto segment_in = chunk_in->get_segment(column_id);

          auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
          DebugAssert(ref_segment_in != nullptr, "All segments should be of type ReferenceSegment.");

          const auto pos_list_in = ref_segment_in->pos_list();

          const auto table_out = ref_segment_in->referenced_table();
          const auto column_id_out = ref_segment_in->referenced_column_id();

          auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

          if (!filtered_pos_list) {
            filtered_pos_list = std::make_shared<PosList>(matches_out->size());
            if (pos_list_in->references_single_chunk()) {
              filtered_pos_list->guarantee_single_chunk();
            }

            size_t offset = 0;
            for (const auto& match : *matches_out) {
              const auto row_id = (*pos_list_in)[match.chunk_offset];
              (*filtered_pos_list)[offset] = row_id;
              ++offset;
            }
          }

          auto ref_segment_out = std::make_shared<ReferenceSegment>(table_out, column_id_out, filtered_pos_list);
          out_segments.push_back(ref_segment_out);
        }
      } else {
        matches_out->guarantee_single_chunk();
        for (ColumnID column_id{0u}; column_id < in_table->column_count(); ++column_id) {
          auto ref_segment_out = std::make_shared<ReferenceSegment>(in_table, column_id, matches_out);
          out_segments.push_back(ref_segment_out);
        }
      }

      std::lock_guard<std::mutex> lock(output_mutex);
      output_table->append_chunk(out_segments, chunk_guard->get_allocator(), chunk_guard->access_counter());
    });

    jobs.push_back(job_task);
    job_task->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return output_table;
}

std::shared_ptr<AbstractExpression> TableScan::_resolve_uncorrelated_subqueries(
    const std::shared_ptr<AbstractExpression>& predicate) {
  // If the predicate has an uncorrelated subquery as an argument, we resolve that subquery first. That way, we can
  // use, e.g., a regular ColumnVsValueTableScanImpl instead of the ExpressionEvaluator. That is faster. We do not care
  // about subqueries that are deeper within the expression tree, because we would need the ExpressionEvaluator for
  // those complex queries anyway.

  if (!std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate) &&
      !std::dynamic_pointer_cast<IsNullExpression>(predicate) &&
      !std::dynamic_pointer_cast<BetweenExpression>(predicate)) {
    // We have no dedicated Impl for these, so we leave them untouched
    return predicate;
  }

  const auto new_predicate = predicate->deep_copy();
  for (auto& argument : new_predicate->arguments) {
    const auto subquery = std::dynamic_pointer_cast<PQPSelectExpression>(argument);
    if (!subquery || subquery->is_correlated()) continue;

    auto subquery_result = AllTypeVariant{};
    resolve_data_type(subquery->data_type(), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      auto expression_result = ExpressionEvaluator{}.evaluate_expression_to_result<ColumnDataType>(*subquery);
      Assert(expression_result->size() == 1, "Expected subquery to return a single row");
      if (!expression_result->is_null(0)) {
        subquery_result = AllTypeVariant{expression_result->value(0)};
      }
    });
    argument = std::make_shared<ValueExpression>(std::move(subquery_result));
  }

  return new_predicate;
}

std::unique_ptr<AbstractTableScanImpl> TableScan::create_impl() const {
  /**
   * Select the scanning implementation (`_impl`) to use based on the kind of the expression. For this we have to
   * closely examine the predicate expression.
   * Use the ExpressionEvaluator as a powerful, but slower fallback if no dedicated scanning implementation exists for
   * an expression.
   */

  auto resolved_predicate = _resolve_uncorrelated_subqueries(_predicate);

  if (const auto binary_predicate_expression =
          std::dynamic_pointer_cast<BinaryPredicateExpression>(resolved_predicate)) {
    const auto predicate_condition = binary_predicate_expression->predicate_condition;

    const auto left_operand = binary_predicate_expression->left_operand();
    const auto right_operand = binary_predicate_expression->right_operand();

    const auto left_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(left_operand);
    const auto right_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(right_operand);

    auto left_value = std::optional<AllTypeVariant>{};
    auto right_value = std::optional<AllTypeVariant>{};

    if (const auto left_value_expression = std::dynamic_pointer_cast<ValueExpression>(left_operand)) {
      left_value = left_value_expression->value;
    }
    if (const auto left_parameter_expression = std::dynamic_pointer_cast<CorrelatedParameterExpression>(left_operand)) {
      left_value = left_parameter_expression->value();
    }
    if (const auto right_value_expression = std::dynamic_pointer_cast<ValueExpression>(right_operand)) {
      right_value = right_value_expression->value;
    }
    if (const auto right_parameter_expression =
            std::dynamic_pointer_cast<CorrelatedParameterExpression>(right_operand)) {
      right_value = right_parameter_expression->value();
    }

    if (left_value && left_value->type() == typeid(NullValue)) left_value.reset();
    if (right_value && right_value->type() == typeid(NullValue)) right_value.reset();

    const auto is_like_predicate =
        predicate_condition == PredicateCondition::Like || predicate_condition == PredicateCondition::NotLike;

    // Predicate pattern: <column> LIKE <non-null value>
    if (left_column_expression && left_column_expression->data_type() == DataType::String && is_like_predicate &&
        right_value) {
      return std::make_unique<ColumnLikeTableScanImpl>(input_table_left(), left_column_expression->column_id,
                                                       predicate_condition,
                                                       type_cast_variant<std::string>(*right_value));
    }

    // Predicate pattern: <column> <binary predicate_condition> <non-null value>
    if (left_column_expression && right_value) {
      return std::make_unique<ColumnVsValueTableScanImpl>(input_table_left(), left_column_expression->column_id,
                                                          predicate_condition, *right_value);
    }
    if (right_column_expression && left_value) {
      return std::make_unique<ColumnVsValueTableScanImpl>(input_table_left(), right_column_expression->column_id,
                                                          flip_predicate_condition(predicate_condition), *left_value);
    }

    // Predicate pattern: <column> <binary predicate_condition> <column>
    if (left_column_expression && right_column_expression) {
      return std::make_unique<ColumnVsColumnTableScanImpl>(input_table_left(), left_column_expression->column_id,
                                                           predicate_condition, right_column_expression->column_id);
    }
  }

  if (const auto is_null_expression = std::dynamic_pointer_cast<IsNullExpression>(resolved_predicate)) {
    // Predicate pattern: <column> IS NULL
    if (const auto left_column_expression =
            std::dynamic_pointer_cast<PQPColumnExpression>(is_null_expression->operand())) {
      return std::make_unique<ColumnIsNullTableScanImpl>(input_table_left(), left_column_expression->column_id,
                                                         is_null_expression->predicate_condition);
    }
  }

  if (const auto between_expression = std::dynamic_pointer_cast<BetweenExpression>(resolved_predicate)) {
    const auto left_column = std::dynamic_pointer_cast<PQPColumnExpression>(between_expression->value());

    const auto lower_bound_value = expression_get_value_or_parameter(*between_expression->lower_bound());
    const auto upper_bound_value = expression_get_value_or_parameter(*between_expression->upper_bound());

    // Predicate pattern: <column> BETWEEN <value-of-type-x> AND <value-of-type-x>
    if (left_column && lower_bound_value && upper_bound_value &&
        lower_bound_value->type() == upper_bound_value->type()) {
      return std::make_unique<ColumnBetweenTableScanImpl>(input_table_left(), left_column->column_id,
                                                          *lower_bound_value, *upper_bound_value);
    }
  }

  // Predicate pattern: Everything else. Fall back to ExpressionEvaluator
  return std::make_unique<ExpressionEvaluatorTableScanImpl>(input_table_left(), resolved_predicate);
}

void TableScan::_on_cleanup() { _impl.reset(); }

}  // namespace opossum
