#include "operator_feature_exporter.hpp"

#include <boost/algorithm/string.hpp>
#include <magic_enum.hpp>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_writer.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/pqp_utils.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

OperatorFeatureExporter::OperatorFeatureExporter(const std::string& path_to_dir)
    : _path_to_dir(path_to_dir),
      _output_path(path_to_dir + "/operators.csv"),
      _join_output_path(_path_to_dir + "/joins.csv"),
      _join_stages_output_path(path_to_dir + "/join_stages.csv"),
      _query_output_path(path_to_dir + "/queries.csv") {}

void OperatorFeatureExporter::export_to_csv(const std::shared_ptr<const AbstractOperator> op) {
  std::lock_guard<std::mutex> lock(_mutex);
  _current_query_hash = "";
  _export_to_csv(op);
}

void OperatorFeatureExporter::export_to_csv(const std::shared_ptr<const AbstractOperator> op,
                                            const std::string& query) {
  std::vector<std::string> query_parts;
  boost::algorithm::split(query_parts, query, boost::algorithm::is_any_of(";"));
  auto trimmed_query =
      boost::algorithm::join(std::vector<std::string>(query_parts.begin(), query_parts.end() - 1), ";");
  std::stringstream query_hex_hash;
  query_hex_hash << std::hex << std::hash<std::string>{}(trimmed_query);
  auto query_single_line{trimmed_query};
  query_single_line.erase(std::remove(query_single_line.begin(), query_single_line.end(), '\n'),
                          query_single_line.end());
  std::lock_guard<std::mutex> lock(_mutex);
  _current_query_hash = pmr_string{query_hex_hash.str()};
  _query_table->append({_current_query_hash, pmr_string{query_single_line}});
  _export_to_csv(op);
}

void OperatorFeatureExporter::_export_to_csv(const std::shared_ptr<const AbstractOperator>& op) {
  _cardinality_estimator = std::make_shared<CardinalityEstimator>();
  visit_pqp(op, [&](const auto& node) {
    //skip Insert, Update, Delete, ...
    if (op->performance_data->has_output) {
      _export_operator(node);
    }
    return PQPVisitation::VisitInputs;
  });
}

void OperatorFeatureExporter::flush() {
  std::lock_guard<std::mutex> lock(_mutex);
  CsvWriter::write(*_general_output_table, _output_path);
  CsvWriter::write(*_join_output_table, _join_output_path);
  CsvWriter::write(*_join_stages_table, _join_stages_output_path);
  CsvWriter::write(*_query_table, _query_output_path);
}

void OperatorFeatureExporter::_export_operator(const std::shared_ptr<const AbstractOperator>& op) {
  switch (op->type()) {
    case OperatorType::Aggregate:
      _export_aggregate(static_pointer_cast<const AbstractAggregateOperator>(op));
      break;
    case OperatorType::GetTable:
      _export_get_table(static_pointer_cast<const GetTable>(op));
      break;
    case OperatorType::JoinHash:
    case OperatorType::JoinSortMerge:
    case OperatorType::JoinNestedLoop:
      _export_join(static_pointer_cast<const AbstractJoinOperator>(op));
      break;
    case OperatorType::TableScan:
      _export_table_scan(static_pointer_cast<const TableScan>(op));
      break;
    case OperatorType::IndexScan:
      _export_index_scan(static_pointer_cast<const IndexScan>(op));
      break;
    default:
      //_export_general_operator(op);
      break;
  }
}

void OperatorFeatureExporter::_export_general_operator(const std::shared_ptr<const AbstractOperator>& op) {
  const auto& operator_info = _general_operator_information(op);
  pmr_string table_name{};
  pmr_string column_name{};
  pmr_string column_type{};
  const auto node = op->lqp_node;

  for (const auto& el : node->node_expressions) {
    if (el->type == ExpressionType::LQPColumn) {
      const auto column_expression = static_pointer_cast<LQPColumnExpression>(el);
      const auto original_node = column_expression->original_node.lock();
      const auto& table_column_information = _table_column_information(node, column_expression);
      table_name = table_column_information.table_name;
      column_name = table_column_information.column_name;
      column_type = table_column_information.column_type;
    }

    const auto output_row = std::vector<AllTypeVariant>{operator_info.name,
                                                        operator_info.left_input_rows,
                                                        operator_info.left_input_columns,
                                                        operator_info.estimated_left_input_rows,
                                                        operator_info.output_rows,
                                                        operator_info.output_columns,
                                                        operator_info.estimated_cardinality,
                                                        operator_info.walltime,
                                                        column_type,
                                                        table_name,
                                                        column_name,
                                                        "",
                                                        int64_t{0}};
    _general_output_table->append(output_row);
  }
}

void OperatorFeatureExporter::_export_aggregate(const std::shared_ptr<const AbstractAggregateOperator>& op) {
  const auto& operator_info = _general_operator_information(op);
  const auto node = op->lqp_node;
  const pmr_string input_sorted =
      op->groupby_column_ids().size() > 0
          ? _check_column_sorted(op->left_input()->performance_data, op->groupby_column_ids().at(0))
          : "";

  for (const auto& el : node->node_expressions) {
    if (el->type == ExpressionType::LQPColumn) {
      const auto column_expression = static_pointer_cast<LQPColumnExpression>(el);
      const auto original_node = column_expression->original_node.lock();
      const auto& table_column_information = _table_column_information(node, column_expression);

      const auto output_row = std::vector<AllTypeVariant>{pmr_string{"Aggregate"},
                                                          operator_info.left_input_rows,
                                                          operator_info.left_input_columns,
                                                          operator_info.estimated_left_input_rows,
                                                          operator_info.output_rows,
                                                          operator_info.output_columns,
                                                          operator_info.estimated_cardinality,
                                                          operator_info.walltime,
                                                          table_column_information.column_type,
                                                          table_column_information.table_name,
                                                          table_column_information.column_name,
                                                          operator_info.name,
                                                          input_sorted,
                                                          _current_query_hash,
                                                          operator_info.left_input_chunks,
                                                          "",
                                                          int64_t{0}};
      _general_output_table->append(output_row);
    }
  }
}

void OperatorFeatureExporter::_export_join(const std::shared_ptr<const AbstractJoinOperator>& op) {
  const auto& operator_info = _general_operator_information(op);
  const auto join_mode = pmr_string{join_mode_to_string.left.at(op->mode())};
  _export_join_stages(op);
  pmr_string left_table_name{};
  pmr_string right_table_name{};
  pmr_string left_column_name{};
  pmr_string right_column_name{};
  pmr_string left_column_type{};
  pmr_string right_column_type{};
  pmr_string left_column_sorted{};
  pmr_string right_column_sorted{};
  int64_t left_distinct_values = -1;
  int64_t right_distinct_values = -1;

  const auto node = op->lqp_node;
  const auto join_node = static_pointer_cast<const JoinNode>(node);
  // const auto operator_predicate = OperatorJoinPredicate::from_expression(*(join_node->node_expressions[0]),
  //                                                                       *node->left_input(), *node->right_input());
  const auto& operator_predicate = op->primary_predicate();

  const auto predicate_expression =
      static_pointer_cast<const AbstractPredicateExpression>(join_node->node_expressions[0]);

  auto predicate_condition = operator_predicate.predicate_condition;  //.value().predicate_condition;
  if (operator_predicate.is_flipped()) {
    predicate_condition = flip_predicate_condition(predicate_condition);
  }
  const auto predicate_condition_string = pmr_string{predicate_condition_to_string.left.at(predicate_condition)};

  const auto first_predicate_expression = predicate_expression->arguments[0];
  if (first_predicate_expression->type == ExpressionType::LQPColumn) {
    const auto left_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(first_predicate_expression);
    const auto& left_table_column_information =
        _table_column_information(node, left_column_expression, InputSide::Left);
    left_table_name = left_table_column_information.table_name;
    left_column_name = left_table_column_information.column_name;
    left_column_type = left_table_column_information.column_type;
    left_column_sorted = _check_column_sorted(op->left_input()->performance_data, operator_predicate.column_ids.first);
  }

  const auto second_predicate_expression = predicate_expression->arguments[1];
  if (second_predicate_expression->type == ExpressionType::LQPColumn) {
    const auto right_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(second_predicate_expression);
    const auto& right_table_column_information =
        _table_column_information(node, right_column_expression, InputSide::Right);
    right_table_name = right_table_column_information.table_name;
    right_column_name = right_table_column_information.column_name;
    right_column_type = right_table_column_information.column_type;
    right_column_sorted =
        _check_column_sorted(op->right_input()->performance_data, operator_predicate.column_ids.second);
  }

  const auto column_ids = operator_predicate.column_ids;
  const auto left_input_statistics = _cardinality_estimator->estimate_statistics(node->left_input());
  const auto right_input_statistics = _cardinality_estimator->estimate_statistics(node->right_input());
  const auto left_data_type = left_input_statistics->column_data_type(column_ids.first);

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto left_column_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(
        left_input_statistics->column_statistics[column_ids.first]);
    const auto right_column_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(
        right_input_statistics->column_statistics[column_ids.second]);
    const auto left_histogram = left_column_statistics->histogram;
    const auto right_histogram = right_column_statistics->histogram;
    if (left_histogram) left_distinct_values = static_cast<int64_t>(left_histogram->total_distinct_count());
    if (right_histogram) right_distinct_values = static_cast<int64_t>(right_histogram->total_distinct_count());
  });

  const auto mode = op->mode();
  const auto operator_flipped_inputs = static_cast<int32_t>(
      op->type() == OperatorType::JoinHash &&
      (mode == JoinMode::Left || mode == JoinMode::AntiNullAsTrue || mode == JoinMode::AntiNullAsFalse ||
       mode == JoinMode::Semi ||
       (mode == JoinMode::Inner && operator_info.left_input_rows > operator_info.right_input_rows)));

  auto output_row = std::vector<AllTypeVariant>{_current_join_id,
                                                operator_info.name,
                                                join_mode,
                                                operator_info.left_input_rows,
                                                operator_info.right_input_rows,
                                                operator_info.left_input_columns,
                                                operator_info.right_input_columns,
                                                operator_info.estimated_left_input_rows,
                                                operator_info.estimated_right_input_rows,
                                                left_distinct_values,
                                                right_distinct_values,
                                                operator_info.output_rows,
                                                operator_info.output_columns,
                                                operator_info.estimated_cardinality,
                                                operator_info.walltime,
                                                left_table_name,
                                                left_column_name,
                                                left_column_type,
                                                right_table_name,
                                                right_column_name,
                                                right_column_type,
                                                operator_flipped_inputs,
                                                left_column_sorted,
                                                right_column_sorted,
                                                _current_query_hash,
                                                operator_info.left_input_chunks,
                                                operator_info.right_input_chunks};

  // Check if the join predicate has been switched (hence, it differs between LQP and PQP) which is done when
  // table A and B are joined but the join predicate is "flipped" (e.g., b.x = a.x). The effect of flipping is that
  // the predicates are in the order (left/right) as the join input tables are.
  if (operator_predicate.is_flipped()) {
    output_row[7] = operator_info.estimated_right_input_rows;
    output_row[8] = operator_info.estimated_left_input_rows;
    output_row[9] = right_distinct_values;
    output_row[10] = left_distinct_values;
    output_row[15] = right_table_name;
    output_row[16] = right_column_name;
    output_row[17] = right_column_type;
    output_row[18] = left_table_name;
    output_row[19] = left_column_name;
    output_row[20] = left_column_type;
    output_row[22] = right_column_sorted;
    output_row[23] = left_column_sorted;
    output_row[25] = operator_info.right_input_chunks;
    output_row[26] = operator_info.left_input_chunks;
  }

  _join_output_table->append(output_row);
  ++_current_join_id;
}

void OperatorFeatureExporter::_export_get_table(const std::shared_ptr<const GetTable>& op) {
  const auto& operator_info = _general_operator_information(op);

  const auto output_row = std::vector<AllTypeVariant>{operator_info.name,
                                                      operator_info.left_input_rows,
                                                      operator_info.left_input_columns,
                                                      operator_info.estimated_left_input_rows,
                                                      operator_info.output_rows,
                                                      operator_info.output_columns,
                                                      operator_info.estimated_cardinality,
                                                      operator_info.walltime,
                                                      "",
                                                      pmr_string{op->table_name()},
                                                      "",
                                                      "",
                                                      "",
                                                      _current_query_hash,
                                                      operator_info.left_input_chunks,
                                                      "",
                                                      int64_t{0}};

  _general_output_table->append(output_row);
}

void OperatorFeatureExporter::_export_table_scan(const std::shared_ptr<const TableScan>& op) {
  const auto& operator_info = _general_operator_information(op);
  Assert(op->_impl_description != "Unset", "Expected TableScan to be executed.");
  const auto implementation = pmr_string{op->_impl_description};
  const auto node = op->lqp_node;
  const auto predicate_node = static_pointer_cast<const PredicateNode>(node);
  const auto predicate = predicate_node->predicate();
  const pmr_string input_sorted = _find_input_sorted(op->left_input()->performance_data, op->predicate());
  pmr_string predicate_str{};

  if (const auto predicate_expression = std::dynamic_pointer_cast<AbstractPredicateExpression>(predicate)) {
    predicate_str = pmr_string{magic_enum::enum_name(predicate_expression->predicate_condition)};
  }

  const auto& performance_data = static_cast<TableScan::PerformanceData&>(*(op->performance_data));
  const auto skipped_chunks = static_cast<int64_t>(performance_data.chunk_scans_skipped);

  // We iterate through the expression until we find the desired column being scanned. This works acceptably ok
  // for most scans we are interested in (e.g., visits both columns of a column vs column scan).
  visit_expression(predicate, [&](const auto& expression) {
    if (expression->type == ExpressionType::LQPColumn) {
      const auto column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
      const auto& table_column_information = _table_column_information(node, column_expression);
      const auto output_row = std::vector<AllTypeVariant>{operator_info.name,
                                                          operator_info.left_input_rows,
                                                          operator_info.left_input_columns,
                                                          operator_info.estimated_left_input_rows,
                                                          operator_info.output_rows,
                                                          operator_info.output_columns,
                                                          operator_info.estimated_cardinality,
                                                          operator_info.walltime,
                                                          table_column_information.column_type,
                                                          table_column_information.table_name,
                                                          table_column_information.column_name,
                                                          implementation,
                                                          input_sorted,
                                                          _current_query_hash,
                                                          operator_info.left_input_chunks,
                                                          predicate_str,
                                                          skipped_chunks};
      _general_output_table->append(output_row);
    }
    return ExpressionVisitation::VisitArguments;
  });
}

void OperatorFeatureExporter::_export_index_scan(const std::shared_ptr<const IndexScan>& op) {
  const auto& operator_info = _general_operator_information(op);
  const auto node = op->lqp_node;
  const auto predicate_node = static_pointer_cast<const PredicateNode>(node);
  const auto predicate = predicate_node->predicate();
  //const pmr_string input_sorted = _find_input_sorted(op->left_input()->get_output(), op->predicate());

  // We iterate through the expression until we find the desired column being scanned. This works acceptably ok
  // for most scans we are interested in (e.g., visits both columns of a column vs column scan).
  visit_expression(predicate, [&](const auto& expression) {
    if (expression->type == ExpressionType::LQPColumn) {
      const auto column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
      const auto& table_column_information = _table_column_information(node, column_expression);
      const auto output_row = std::vector<AllTypeVariant>{operator_info.name,
                                                          operator_info.left_input_rows,
                                                          operator_info.left_input_columns,
                                                          operator_info.estimated_left_input_rows,
                                                          operator_info.output_rows,
                                                          operator_info.output_columns,
                                                          operator_info.estimated_cardinality,
                                                          operator_info.walltime,
                                                          table_column_information.column_type,
                                                          table_column_information.table_name,
                                                          table_column_information.column_name,
                                                          operator_info.name,
                                                          "",
                                                          _current_query_hash,
                                                          operator_info.left_input_chunks,
                                                          "",
                                                          int64_t{0}};
      _general_output_table->append(output_row);
    }
    return ExpressionVisitation::VisitArguments;
  });
}

void OperatorFeatureExporter::_export_join_stages(const std::shared_ptr<const AbstractJoinOperator>& op) {
  if (const auto join_operator = std::dynamic_pointer_cast<const JoinHash>(op)) {
    const auto& performance_data =
        dynamic_cast<OperatorPerformanceData<JoinHash::OperatorSteps>&>(*(join_operator->performance_data));
    constexpr auto steps = magic_enum::enum_entries<JoinHash::OperatorSteps>();

    for (const auto& step : steps) {
      const auto runtime = static_cast<int64_t>(performance_data.get_step_runtime(step.first).count());
      _join_stages_table->append({static_cast<int32_t>(_current_join_id), pmr_string{step.second}, runtime});
    }
  }
}

const OperatorFeatureExporter::TableColumnInformation OperatorFeatureExporter::_table_column_information(
    const std::shared_ptr<const AbstractLQPNode>& lqp_node,
    const std::shared_ptr<const LQPColumnExpression>& column_expression, const InputSide input_side) const {
  std::string table_name{};
  pmr_string column_name{};
  pmr_string column_type{};

  const auto original_column_id = column_expression->original_column_id;
  const auto original_node = column_expression->original_node.lock();

  if (original_node->type == LQPNodeType::StoredTable) {
    const auto stored_table_node = static_pointer_cast<const StoredTableNode>(original_node);
    table_name = stored_table_node->table_name;

    const auto input = input_side == InputSide::Left ? lqp_node->left_input() : lqp_node->right_input();
    if (original_node == input) {
      column_type = "DATA";
    } else {
      column_type = "REFERENCE";
    }

    if (!Hyrise::get().storage_manager.has_table(table_name)) {
      return TableColumnInformation(pmr_string{table_name}, column_name, column_type);
    }

    const auto original_table = Hyrise::get().storage_manager.get_table(table_name);
    if (original_column_id != INVALID_COLUMN_ID) {
      column_name = pmr_string{original_table->column_name(original_column_id)};
    } else {
      column_name = "COUNT(*)";
    }
  }

  return TableColumnInformation(pmr_string{table_name}, column_name, column_type);
}

const OperatorFeatureExporter::GeneralOperatorInformation OperatorFeatureExporter::_general_operator_information(
    const std::shared_ptr<const AbstractOperator>& op) const {
  GeneralOperatorInformation operator_info;
  operator_info.name = pmr_string{op->name()};

  if (op->left_input()) {
    operator_info.left_input_rows = static_cast<int64_t>(op->left_input()->performance_data->output_row_count);
    operator_info.left_input_columns = static_cast<int32_t>(op->left_input()->performance_data->output_column_count);
    operator_info.left_input_chunks = op->left_input()->performance_data->output_chunk_count;
  }
  if (op->right_input()) {
    operator_info.right_input_rows = static_cast<int64_t>(op->right_input()->performance_data->output_row_count);
    operator_info.right_input_columns = static_cast<int32_t>(op->right_input()->performance_data->output_column_count);
    operator_info.right_input_chunks = op->right_input()->performance_data->output_chunk_count;
  }

  const auto lqp_node = op->lqp_node;
  if (lqp_node->left_input()) {
    operator_info.estimated_left_input_rows = _cardinality_estimator->estimate_cardinality(lqp_node->left_input());
  }

  if (lqp_node->right_input()) {
    operator_info.estimated_right_input_rows = _cardinality_estimator->estimate_cardinality(lqp_node->right_input());
  }

  operator_info.output_rows = static_cast<int64_t>(op->performance_data->output_row_count);
  operator_info.walltime = static_cast<int64_t>(op->performance_data->walltime.count());
  operator_info.output_columns = static_cast<int32_t>(op->performance_data->output_column_count);
  operator_info.estimated_cardinality = _cardinality_estimator->estimate_cardinality(lqp_node);

  return operator_info;
}

const pmr_string OperatorFeatureExporter::_check_column_sorted(
    const std::unique_ptr<AbstractOperatorPerformanceData>& performance_data, const ColumnID column_id) const {
  bool sorted_ascending = true;
  bool sorted_descending = true;

  if (!performance_data) {
    return pmr_string{};
  }
  if (column_id > performance_data->output_column_count) {
    return pmr_string{};
  }

  for (const auto& chunk_sorted_by : performance_data->chunks_sorted_by) {
    if (!(sorted_ascending || sorted_descending)) break;

    if (chunk_sorted_by.empty()) {
      sorted_ascending = false;
      sorted_descending = false;
      break;
    }

    bool chunk_sorted_ascending = false;
    bool chunk_sorted_descending = false;
    for (const auto& sort_definition : chunk_sorted_by) {
      if (sort_definition.column == column_id) {
        if (sort_definition.sort_mode == SortMode::Ascending) {
          chunk_sorted_ascending = true;
        } else
          chunk_sorted_descending = true;
      }
    }
    sorted_ascending &= chunk_sorted_ascending;
    sorted_descending &= chunk_sorted_descending;
  }
  if (sorted_ascending) return pmr_string{"Ascending"};
  if (sorted_descending) return pmr_string{"Descending"};
  return pmr_string{"No"};
}

const pmr_string OperatorFeatureExporter::_find_input_sorted(
    const std::unique_ptr<AbstractOperatorPerformanceData>& performance_data,
    const std::shared_ptr<AbstractExpression>& predicate) const {
  auto input_sorted = pmr_string{};
  visit_expression(predicate, [&](const auto& expression) {
    if (expression->type == ExpressionType::PQPColumn) {
      const auto column_expression = std::static_pointer_cast<PQPColumnExpression>(expression);
      input_sorted = _check_column_sorted(performance_data, column_expression->column_id);
      return ExpressionVisitation::DoNotVisitArguments;
    }
    return ExpressionVisitation::VisitArguments;
  });
  return input_sorted;
}

}  // namespace opossum
