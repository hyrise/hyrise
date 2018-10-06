#include "table_scan.hpp"

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "constant_mappings.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
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
#include "table_scan/column_comparison_table_scan_impl.hpp"
#include "table_scan/expression_evaluator_table_scan_impl.hpp"
#include "table_scan/is_null_table_scan_impl.hpp"
#include "table_scan/like_table_scan_impl.hpp"
#include "table_scan/single_column_table_scan_impl.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in,
                     const std::shared_ptr<AbstractExpression>& predicate)
    : AbstractReadOnlyOperator{OperatorType::TableScan, in}, _predicate(predicate) {}

TableScan::~TableScan() = default;

void TableScan::set_excluded_chunk_ids(const std::vector<ChunkID>& chunk_ids) { _excluded_chunk_ids = chunk_ids; }

const std::shared_ptr<AbstractExpression>& TableScan::predicate() const { return _predicate; }

const std::string TableScan::name() const { return "TableScan"; }

const std::string TableScan::description(DescriptionMode description_mode) const {
  return name() + _predicate->as_column_name();
}

void TableScan::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  expressions_set_transaction_context({_predicate}, transaction_context);
}

void TableScan::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters){
  expression_set_parameters(_predicate, parameters);
}

std::shared_ptr<AbstractOperator> TableScan::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<TableScan>(copied_input_left, _predicate->deep_copy());
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  _in_table = input_table_left();

  const auto impl = _get_impl();

  _output_table = std::make_shared<Table>(_in_table->column_definitions(), TableType::References);

  std::mutex output_mutex;

  const auto excluded_chunk_set = std::unordered_set<ChunkID>{_excluded_chunk_ids.cbegin(), _excluded_chunk_ids.cend()};

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(_in_table->chunk_count() - excluded_chunk_set.size());

  for (ChunkID chunk_id{0u}; chunk_id < _in_table->chunk_count(); ++chunk_id) {
    if (excluded_chunk_set.count(chunk_id)) continue;

    auto job_task = std::make_shared<JobTask>([=, &output_mutex, &impl]() {
      const auto chunk_guard = _in_table->get_chunk_with_access_counting(chunk_id);
      // The actual scan happens in the sub classes of BaseTableScanImpl
      const auto matches_out = impl->scan_chunk(chunk_id);
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
      if (_in_table->type() == TableType::References) {
        const auto chunk_in = _in_table->get_chunk(chunk_id);

        auto filtered_pos_lists = std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>>{};

        for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
          auto segment_in = chunk_in->get_segment(column_id);

          auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
          DebugAssert(ref_segment_in != nullptr, "All segments should be of type ReferenceSegment.");

          const auto pos_list_in = ref_segment_in->pos_list();

          const auto table_out = ref_segment_in->referenced_table();
          const auto column_id_out = ref_segment_in->referenced_column_id();

          auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

          if (!filtered_pos_list) {
            filtered_pos_list = std::make_shared<PosList>();
            filtered_pos_list->reserve(matches_out->size());

            for (const auto& match : *matches_out) {
              const auto row_id = (*pos_list_in)[match.chunk_offset];
              filtered_pos_list->push_back(row_id);
            }
          }

          auto ref_segment_out = std::make_shared<ReferenceSegment>(table_out, column_id_out, filtered_pos_list);
          out_segments.push_back(ref_segment_out);
        }
      } else {
        for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
          auto ref_segment_out = std::make_shared<ReferenceSegment>(_in_table, column_id, matches_out);
          out_segments.push_back(ref_segment_out);
        }
      }

      std::lock_guard<std::mutex> lock(output_mutex);
      _output_table->append_chunk(out_segments, chunk_guard->get_allocator(), chunk_guard->access_counter());
    });

    jobs.push_back(job_task);
    job_task->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return _output_table;
}

std::unique_ptr<AbstractTableScanImpl> TableScan::_get_impl() const {
  /**
   * Select the scanning implementation (`_impl`) to use based on the kind of the expression. Use the
   * ExpressionEvaluator as a fallback, if no dedicated implementation exists for an expression.
   */

  const auto binary_predicate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(_predicate);
  if (binary_predicate_expression) {
    const auto operator_scan_predicates = OperatorScanPredicate::from_expression()

    const auto predicate_condition = binary_predicate_expression->predicate_condition;
    const auto left_column_expression =
        std::dynamic_pointer_cast<PQPColumnExpression>(binary_predicate_expression->left_operand());
    const auto right_column_expression =
        std::dynamic_pointer_cast<PQPColumnExpression>(binary_predicate_expression->right_operand());
    const auto right_value_expression =
        std::dynamic_pointer_cast<ValueExpression>(binary_predicate_expression->right_operand());

    if (left_column_expression) {
      if ((predicate_condition == PredicateCondition::Like || predicate_condition == PredicateCondition::NotLike) &&
          right_value_expression) {
        Assert(left_column_expression->data_type() == DataType::String,
               "LIKE operator only applicable on string columns.");
        DebugAssert(!variant_is_null(right_value_expression->value), "Right value must not be NULL.");

        const auto right_wildcard = type_cast<std::string>(right_value_expression->value);

        return std::make_unique<LikeTableScanImpl>(_in_table, left_column_expression->column_id, predicate_condition,
                                                    right_wildcard);

      } else if (predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
        return std::make_unique<IsNullTableScanImpl>(_in_table, left_column_expression->column_id,
                                                      predicate_condition);

      } else if (right_value_expression) {
        return std::make_unique<SingleColumnTableScanImpl>(_in_table, left_column_expression->column_id, predicate_condition, right_value_expression->value);

      } else if (right_column_expression) {
        return std::make_unique<ColumnComparisonTableScanImpl>(_in_table, left_column_expression->column_id, predicate_condition,
                                                                right_column_expression->column_id);

      }
    }
  }

  return std::make_unique<ExpressionEvaluatorTableScanImpl>(_in_table, _predicate);
}

}  // namespace opossum
