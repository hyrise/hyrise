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
#include "hyrise.hpp"
#include "lossless_cast.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "table_scan/column_between_table_scan_impl.hpp"
#include "table_scan/column_is_null_table_scan_impl.hpp"
#include "table_scan/column_like_table_scan_impl.hpp"
#include "table_scan/column_vs_column_table_scan_impl.hpp"
#include "table_scan/column_vs_value_table_scan_impl.hpp"
#include "table_scan/expression_evaluator_table_scan_impl.hpp"
#include "utils/assert.hpp"
#include "utils/lossless_predicate_cast.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in,
                     const std::shared_ptr<AbstractExpression>& predicate)
    : AbstractReadOnlyOperator{OperatorType::TableScan, in}, _predicate(predicate) {}

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

  _impl = create_impl();
  _impl_description = _impl->description();

  std::mutex output_mutex;

  const auto excluded_chunk_set = std::unordered_set<ChunkID>{excluded_chunk_ids.cbegin(), excluded_chunk_ids.cend()};

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
  output_chunks.reserve(in_table->chunk_count() - excluded_chunk_set.size());

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(in_table->chunk_count() - excluded_chunk_set.size());

  const auto chunk_count = in_table->chunk_count();
  for (ChunkID chunk_id{0u}; chunk_id < chunk_count; ++chunk_id) {
    if (excluded_chunk_set.count(chunk_id)) continue;
    const auto chunk_in = in_table->get_chunk(chunk_id);
    Assert(chunk_in, "Did not expect deleted chunk here.");  // see #1686

    // chunk_in – Copy by value since copy by reference is not possible due to the limited scope of the for-iteration.
    auto job_task = std::make_shared<JobTask>([this, chunk_id, chunk_in, &in_table, &output_mutex, &output_chunks]() {
      // The actual scan happens in the sub classes of BaseTableScanImpl
      const auto matches_out = _impl->scan_chunk(chunk_id);
      if (matches_out->empty()) return;

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
        auto filtered_pos_lists = std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>>{};

        for (ColumnID column_id{0u}; column_id < in_table->column_count(); ++column_id) {
          auto segment_in = chunk_in->get_segment(column_id);

          auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
          DebugAssert(ref_segment_in, "All segments should be of type ReferenceSegment.");

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
      output_chunks.emplace_back(std::make_shared<Chunk>(out_segments, nullptr, chunk_in->get_allocator()));
    });

    jobs.push_back(job_task);
    job_task->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);

  return std::make_shared<Table>(in_table->column_definitions(), TableType::References, std::move(output_chunks));
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
    const auto subquery = std::dynamic_pointer_cast<PQPSubqueryExpression>(argument);
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
    auto predicate_condition = binary_predicate_expression->predicate_condition;

    const auto left_operand = binary_predicate_expression->left_operand();
    const auto right_operand = binary_predicate_expression->right_operand();

    const auto left_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(left_operand);
    const auto right_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(right_operand);

    auto left_value = expression_get_value_or_parameter(*left_operand);
    auto right_value = expression_get_value_or_parameter(*right_operand);

    if (left_value && right_column_expression) {
      // Try to cast the value to the type of the column. This might require adjusting the predicate (see
      // lossless_predicate_cast.hpp for details).

      // lossless_predicate_variant_cast considers the input value to be the right-side value, so we need to flip the
      // condition twice.
      const auto adjusted_predicate_and_value = lossless_predicate_variant_cast(
          flip_predicate_condition(predicate_condition), *left_value, right_column_expression->data_type());
      if (adjusted_predicate_and_value) {
        predicate_condition = flip_predicate_condition(adjusted_predicate_and_value->first);
        left_value = adjusted_predicate_and_value->second;
      } else {
        // Incompatible types
        left_value = std::nullopt;
      }
    }
    if (right_value && left_column_expression) {
      // Same for the other side - no flip needed
      const auto adjusted_predicate_and_value =
          lossless_predicate_variant_cast(predicate_condition, *right_value, left_column_expression->data_type());
      if (adjusted_predicate_and_value) {
        predicate_condition = adjusted_predicate_and_value->first;
        right_value = adjusted_predicate_and_value->second;
      } else {
        right_value = std::nullopt;
      }
    }

    const auto is_like_predicate =
        predicate_condition == PredicateCondition::Like || predicate_condition == PredicateCondition::NotLike;

    // Predicate pattern: <column of type string> LIKE <value of type string>
    if (left_column_expression && left_column_expression->data_type() == DataType::String && is_like_predicate &&
        right_value) {
      return std::make_unique<ColumnLikeTableScanImpl>(input_table_left(), left_column_expression->column_id,
                                                       predicate_condition, boost::get<pmr_string>(*right_value));
    }

    // Predicate pattern: <column of type T> <binary predicate_condition> <value of type T>
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
    auto predicate_condition = between_expression->predicate_condition;
    const auto left_column = std::dynamic_pointer_cast<PQPColumnExpression>(between_expression->value());

    auto [lower_condition, upper_condition] = between_to_conditions(predicate_condition);

    auto lower_bound_value = expression_get_value_or_parameter(*between_expression->lower_bound());
    if (lower_bound_value) {
      const auto adjusted_predicate_and_value =
          lossless_predicate_variant_cast(lower_condition, *lower_bound_value, left_column->data_type());
      if (adjusted_predicate_and_value) {
        lower_condition = adjusted_predicate_and_value->first;
        lower_bound_value = adjusted_predicate_and_value->second;
      } else {
        lower_bound_value = std::nullopt;
      }
    }

    auto upper_bound_value = expression_get_value_or_parameter(*between_expression->upper_bound());
    if (upper_bound_value) {
      const auto adjusted_predicate_and_value =
          lossless_predicate_variant_cast(upper_condition, *upper_bound_value, left_column->data_type());
      if (adjusted_predicate_and_value) {
        upper_condition = adjusted_predicate_and_value->first;
        upper_bound_value = adjusted_predicate_and_value->second;
      } else {
        upper_bound_value = std::nullopt;
      }
    }

    predicate_condition = conditions_to_between(lower_condition, upper_condition);

    // Predicate pattern: <column> BETWEEN <value-of-type-x> AND <value-of-type-x>
    if (left_column && lower_bound_value && upper_bound_value &&
        lower_bound_value->type() == upper_bound_value->type()) {
      return std::make_unique<ColumnBetweenTableScanImpl>(input_table_left(), left_column->column_id,
                                                          *lower_bound_value, *upper_bound_value, predicate_condition);
    }
  }

  // Predicate pattern: Everything else. Fall back to ExpressionEvaluator
  return std::make_unique<ExpressionEvaluatorTableScanImpl>(input_table_left(), resolved_predicate);
}

void TableScan::_on_cleanup() { _impl.reset(); }

}  // namespace opossum
