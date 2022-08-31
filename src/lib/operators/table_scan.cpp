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
#include "storage/abstract_segment.hpp"
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

namespace hyrise {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& input_operator,
                     const std::shared_ptr<AbstractExpression>& predicate)
    : AbstractReadOnlyOperator{OperatorType::TableScan, input_operator, nullptr, std::make_unique<PerformanceData>()},
      _predicate(predicate) {
  /**
   * Register as a consumer for all uncorrelated subqueries.
   * In contrast, we do not register for correlated subqueries which cannot be reused by design. They are fully owned
   * and managed by the ExpressionEvaluator.
   */
  auto pqp_subquery_expressions = find_pqp_subquery_expressions(predicate);
  for (const auto& subquery_expression : pqp_subquery_expressions) {
    if (subquery_expression->is_correlated()) {
      continue;
    }
    /**
     * Uncorrelated subqueries will be resolved when TableScan::create_impl is called. Therefore, we
     * 1. register as a consumer and
     * 2. store pointers to eventually call ExpressionEvaluator::populate_uncorrelated_subquery_results_cache later on.
     */
    subquery_expression->pqp->register_consumer();
    _uncorrelated_subquery_expressions.push_back(subquery_expression);
  }
}

const std::shared_ptr<AbstractExpression>& TableScan::predicate() const {
  return _predicate;
}

const std::string& TableScan::name() const {
  static const auto name = std::string{"TableScan"};
  return name;
}

std::string TableScan::description(DescriptionMode description_mode) const {
  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');

  std::stringstream stream;

  stream << AbstractOperator::description(description_mode) << separator;
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
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<TableScan>(copied_left_input, _predicate->deep_copy(copied_ops));
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto in_table = left_input_table();

  _impl = create_impl();
  _impl_description = _impl->description();

  std::mutex output_mutex;

  const auto excluded_chunk_set = std::unordered_set<ChunkID>{excluded_chunk_ids.cbegin(), excluded_chunk_ids.cend()};

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
  output_chunks.reserve(in_table->chunk_count() - excluded_chunk_set.size());

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(in_table->chunk_count() - excluded_chunk_set.size());

  const auto chunk_count = in_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    if (excluded_chunk_set.contains(chunk_id)) {
      continue;
    }
    const auto chunk_in = in_table->get_chunk(chunk_id);
    Assert(chunk_in, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    // chunk_in – Copy by value since copy by reference is not possible due to the limited scope of the for-iteration.
    auto perform_table_scan = [this, chunk_id, chunk_in, &in_table, &output_mutex, &output_chunks]() {
      // The actual scan happens in the sub classes of BaseTableScanImpl
      const auto matches_out = _impl->scan_chunk(chunk_id);
      if (matches_out->empty()) {
        return;
      }

      const auto column_count = in_table->column_count();
      Segments out_segments;
      out_segments.reserve(column_count);

      /**
       * matches_out contains a list of row IDs into this chunk. If this is not a reference table, we can directly use
       * the matches to construct the reference segments of the output. If it is a reference segment, we need to
       * resolve the row IDs so that they reference the physical data segments (value, dictionary) instead, since we
       * don’t allow multi-level referencing. To save time and space, we want to share position lists between segments
       * as much as possible. Position lists can be shared between two segments iff (a) they point to the same table
       * and (b) the reference segments of the input table point to the same positions in the same order (i.e. they
       * share their position list).
       */
      auto keep_chunk_sort_order = true;
      if (in_table->type() == TableType::References) {
        if (matches_out->size() == chunk_in->size()) {
          // Shortcut - the entire input reference segment matches, so we can simply forward that chunk
          for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
            const auto segment_in = chunk_in->get_segment(column_id);
            out_segments.emplace_back(segment_in);
          }
        } else {
          auto filtered_pos_lists = std::map<std::shared_ptr<const AbstractPosList>, std::shared_ptr<RowIDPosList>>{};

          for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
            const auto segment_in = chunk_in->get_segment(column_id);

            auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
            DebugAssert(ref_segment_in, "All segments should be of type ReferenceSegment.");

            const auto pos_list_in = ref_segment_in->pos_list();

            const auto table_out = ref_segment_in->referenced_table();
            const auto column_id_out = ref_segment_in->referenced_column_id();

            auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

            if (!filtered_pos_list) {
              filtered_pos_list = std::make_shared<RowIDPosList>(matches_out->size());
              if (pos_list_in->references_single_chunk()) {
                filtered_pos_list->guarantee_single_chunk();
              } else {
                // When segments reference multiple chunks, we do not keep the sort order of the input chunk. The main
                // reason is that several table scan implementations split the pos lists by chunks (see
                // AbstractDereferencedColumnTableScanImpl::_scan_reference_segment) and thus shuffle the data. While
                // this does not affect all scan implementations, we chose the safe and defensive path for now.
                keep_chunk_sort_order = false;
              }

              auto offset = size_t{0};
              for (const auto& match : *matches_out) {
                const auto row_id = (*pos_list_in)[match.chunk_offset];
                (*filtered_pos_list)[offset] = row_id;
                ++offset;
              }
            }

            const auto ref_segment_out =
                std::make_shared<ReferenceSegment>(table_out, column_id_out, filtered_pos_list);
            out_segments.push_back(ref_segment_out);
          }
        }
      } else {
        matches_out->guarantee_single_chunk();

        // If the entire chunk is matched, create an EntireChunkPosList instead
        const auto output_pos_list = matches_out->size() == chunk_in->size()
                                         ? static_cast<std::shared_ptr<AbstractPosList>>(
                                               std::make_shared<EntireChunkPosList>(chunk_id, chunk_in->size()))
                                         : static_cast<std::shared_ptr<AbstractPosList>>(matches_out);

        for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
          const auto ref_segment_out = std::make_shared<ReferenceSegment>(in_table, column_id, output_pos_list);
          out_segments.push_back(ref_segment_out);
        }
      }

      const auto chunk = std::make_shared<Chunk>(out_segments, nullptr, chunk_in->get_allocator());
      chunk->finalize();
      if (keep_chunk_sort_order && !chunk_in->individually_sorted_by().empty()) {
        chunk->set_individually_sorted_by(chunk_in->individually_sorted_by());
      }
      std::lock_guard<std::mutex> lock(output_mutex);
      output_chunks.emplace_back(chunk);
    };
    // Spawn job when chunk sufficiently large. The upper bound of the chunk size, still needs to be re-evaluated over
    // time to find the value which gives the best performance.
    constexpr auto JOB_SPAWN_THRESHOLD = ChunkOffset{500};
    if (chunk_in->size() >= JOB_SPAWN_THRESHOLD) {
      auto job_task = std::make_shared<JobTask>(perform_table_scan);
      jobs.push_back(job_task);
    } else {
      perform_table_scan();
    }
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  auto& scan_performance_data = dynamic_cast<PerformanceData&>(*performance_data);
  scan_performance_data.num_chunks_with_early_out = _impl->num_chunks_with_early_out.load();
  scan_performance_data.num_chunks_with_all_rows_matching = _impl->num_chunks_with_all_rows_matching.load();
  scan_performance_data.num_chunks_with_binary_search = _impl->num_chunks_with_binary_search.load();

  return std::make_shared<Table>(in_table->column_definitions(), TableType::References, std::move(output_chunks));
}

std::shared_ptr<const AbstractExpression> TableScan::_resolve_uncorrelated_subqueries(
    const std::shared_ptr<const AbstractExpression>& predicate) {
  /**
   * If the predicate has an uncorrelated subquery as an argument, we resolve that subquery first. That way, we can
   * use, e.g., a regular ColumnVsValueTableScanImpl instead of the ExpressionEvaluator. That is faster. We do not care
   * about subqueries that are deeper within the expression tree, because we would need the ExpressionEvaluator for
   * those complex queries anyway.
   */
  if (!std::dynamic_pointer_cast<const BinaryPredicateExpression>(predicate) &&
      !std::dynamic_pointer_cast<const IsNullExpression>(predicate) &&
      !std::dynamic_pointer_cast<const BetweenExpression>(predicate)) {
    // We have no dedicated Impl for these, so we leave them untouched
    return predicate;
  }

  /**
   * (1) Create arguments for new predicate
   *      - resolve arguments of type uncorrelated subquery, and create ValueExpressions from the results
   *      - create deep copies for all other arguments
   */
  auto arguments_count = predicate->arguments.size();
  auto new_arguments = std::vector<std::shared_ptr<AbstractExpression>>();
  new_arguments.reserve(arguments_count);
  auto computed_subqueries_count = int{0};

  for (auto argument_idx = size_t{0}; argument_idx < arguments_count; ++argument_idx) {
    const auto subquery = std::dynamic_pointer_cast<PQPSubqueryExpression>(predicate->arguments.at(argument_idx));
    if (!subquery || subquery->is_correlated()) {
      new_arguments.emplace_back(predicate->arguments.at(argument_idx)->deep_copy());
      continue;
    }

    auto subquery_result = AllTypeVariant{};
    resolve_data_type(subquery->data_type(), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      auto expression_result = ExpressionEvaluator{}.evaluate_expression_to_result<ColumnDataType>(*subquery);
      Assert(expression_result->size() == 1, "Expected subquery to return a single row");
      if (!expression_result->is_null(0)) {
        subquery_result = AllTypeVariant{expression_result->value(0)};
      }
    });
    new_arguments.emplace_back(std::make_shared<ValueExpression>(std::move(subquery_result)));

    // Deregister, because we obtained the subquery result and no longer need the subquery plan.
    subquery->pqp->deregister_consumer();
    computed_subqueries_count++;
  }
  DebugAssert(new_arguments.size() == predicate->arguments.size(), "Unexpected number of arguments.");
  DebugAssert(static_cast<int>(_uncorrelated_subquery_expressions.size()) == computed_subqueries_count,
              "Expected to resolve all uncorrelated subqueries.");

  // Return original predicate if we did not compute any subquery results
  if (computed_subqueries_count == 0) {
    return predicate;
  }

  /**
   * (2) Create new predicate with arguments from step (1)
   */
  if (auto binary_predicate = std::dynamic_pointer_cast<const BinaryPredicateExpression>(predicate)) {
    auto left_operand = new_arguments.at(0);
    auto right_operand = new_arguments.at(1);
    DebugAssert(left_operand && right_operand, "Unexpected null pointer.");
    return std::make_shared<BinaryPredicateExpression>(binary_predicate->predicate_condition, left_operand,
                                                       right_operand);
  }
  if (auto between_predicate = std::dynamic_pointer_cast<const BetweenExpression>(predicate)) {
    auto value = new_arguments.at(0);
    auto lower_bound = new_arguments.at(1);
    auto upper_bound = new_arguments.at(2);
    DebugAssert(value && lower_bound && upper_bound, "Unexpected null pointer.");
    return std::make_shared<BetweenExpression>(between_predicate->predicate_condition, value, lower_bound, upper_bound);
  }
  if (auto is_null_predicate = std::dynamic_pointer_cast<const IsNullExpression>(predicate)) {
    auto operand = new_arguments.at(0);
    DebugAssert(operand, "Unexpected null pointer.");
    return std::make_shared<IsNullExpression>(is_null_predicate->predicate_condition, operand);
  }

  Fail("Unexpected predicate type");
}

std::unique_ptr<AbstractTableScanImpl> TableScan::create_impl() {
  /**
   * Select the scanning implementation (`_impl`) to use based on the kind of the expression. For this we have to
   * closely examine the predicate expression.
   *
   * Many implementations require the comparison values to be of the same column type. If we were to cast the values
   * to the column type, `int_column = 16.25` would turn into `int_column = 16`, which is obviously wrong. As such, we
   * use lossless casts to guarantee safe type conversions. This was introduced by #1550.
   *
   * Use the ExpressionEvaluator as a powerful, but slower fallback if no dedicated scanning implementation exists for
   * an expression.
   */

  const auto resolved_predicate = _resolve_uncorrelated_subqueries(_predicate);

  if (const auto binary_predicate_expression =
          std::dynamic_pointer_cast<const BinaryPredicateExpression>(resolved_predicate)) {
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
      return std::make_unique<ColumnLikeTableScanImpl>(left_input_table(), left_column_expression->column_id,
                                                       predicate_condition, boost::get<pmr_string>(*right_value));
    }

    // Predicate pattern: <column of type T> <binary predicate_condition> <value of type T>
    if (left_column_expression && right_value) {
      return std::make_unique<ColumnVsValueTableScanImpl>(left_input_table(), left_column_expression->column_id,
                                                          predicate_condition, *right_value);
    }
    if (right_column_expression && left_value) {
      return std::make_unique<ColumnVsValueTableScanImpl>(left_input_table(), right_column_expression->column_id,
                                                          flip_predicate_condition(predicate_condition), *left_value);
    }

    // Predicate pattern: <column> <binary predicate_condition> <column>
    if (left_column_expression && right_column_expression) {
      return std::make_unique<ColumnVsColumnTableScanImpl>(left_input_table(), left_column_expression->column_id,
                                                           predicate_condition, right_column_expression->column_id);
    }
  }

  if (const auto is_null_expression = std::dynamic_pointer_cast<const IsNullExpression>(resolved_predicate)) {
    // Predicate pattern: <column> IS NULL
    if (const auto left_column_expression =
            std::dynamic_pointer_cast<PQPColumnExpression>(is_null_expression->operand())) {
      return std::make_unique<ColumnIsNullTableScanImpl>(left_input_table(), left_column_expression->column_id,
                                                         is_null_expression->predicate_condition);
    }
  }

  if (const auto between_expression = std::dynamic_pointer_cast<const BetweenExpression>(resolved_predicate)) {
    // The ColumnBetweenTableScanImpl expects both values to be of the same data type as the column that is being
    // scanned. We retrieve the lower and upper bounds of the BetweenExpression, perform a
    // lossless_predicate_variant_cast into the column's data type and reassemble the between condition.

    auto predicate_condition = between_expression->predicate_condition;
    const auto left_column = std::dynamic_pointer_cast<PQPColumnExpression>(between_expression->value());

    auto [lower_condition, upper_condition] = between_to_conditions(predicate_condition);

    auto lower_bound_value = expression_get_value_or_parameter(*between_expression->lower_bound());
    if (lower_bound_value) {
      const auto adjusted_predicate_and_value = lossless_predicate_variant_cast(
          lower_condition, *lower_bound_value, between_expression->value()->data_type());
      if (adjusted_predicate_and_value) {
        lower_condition = adjusted_predicate_and_value->first;
        lower_bound_value = adjusted_predicate_and_value->second;
      } else {
        lower_bound_value = std::nullopt;
      }
    }

    auto upper_bound_value = expression_get_value_or_parameter(*between_expression->upper_bound());
    if (upper_bound_value) {
      const auto adjusted_predicate_and_value = lossless_predicate_variant_cast(
          upper_condition, *upper_bound_value, between_expression->value()->data_type());
      if (adjusted_predicate_and_value) {
        upper_condition = adjusted_predicate_and_value->first;
        upper_bound_value = adjusted_predicate_and_value->second;
      } else {
        upper_bound_value = std::nullopt;
      }
    }

    predicate_condition = conditions_to_between(lower_condition, upper_condition);

    // Predicate pattern: <column of type T> BETWEEN <value of type T> AND <value of type T>
    if (left_column && lower_bound_value && upper_bound_value &&
        lower_bound_value->type() == upper_bound_value->type()) {
      return std::make_unique<ColumnBetweenTableScanImpl>(left_input_table(), left_column->column_id,
                                                          *lower_bound_value, *upper_bound_value, predicate_condition);
    }
  }

  // Predicate pattern: Everything else. Fall back to ExpressionEvaluator.
  const auto& uncorrelated_subquery_results =
      ExpressionEvaluator::populate_uncorrelated_subquery_results_cache(_uncorrelated_subquery_expressions);
  // Deregister, because we obtained the results and no longer need the subquery plans.
  for (const auto& pqp_subquery_expression : _uncorrelated_subquery_expressions) {
    pqp_subquery_expression->pqp->deregister_consumer();
  }
  return std::make_unique<ExpressionEvaluatorTableScanImpl>(left_input_table(), resolved_predicate,
                                                            uncorrelated_subquery_results);
}

void TableScan::_on_cleanup() {
  _impl.reset();
}

}  // namespace hyrise
