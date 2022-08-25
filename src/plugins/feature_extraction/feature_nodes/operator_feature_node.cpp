#include "operator_feature_node.hpp"

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "feature_extraction/feature_nodes/aggregate_function_feature_node.hpp"
#include "feature_extraction/feature_nodes/base_table_feature_node.hpp"
#include "feature_extraction/feature_nodes/predicate_feature_node.hpp"
#include "feature_extraction/util/feature_extraction_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "operators/operator_join_predicate.hpp"

namespace {

using namespace hyrise;

std::shared_ptr<OperatorFeatureNode> _recursively_build_graph_from_pqp(
    const std::shared_ptr<const AbstractOperator>& op) {
  const auto left_input = op->left_input() ? _recursively_build_graph_from_pqp(op->left_input()) : nullptr;
  const auto right_input = op->right_input() ? _recursively_build_graph_from_pqp(op->right_input()) : nullptr;

  return std::make_shared<OperatorFeatureNode>(op, left_input, right_input);
}

std::shared_ptr<PredicateFeatureNode> resolve_join_predicate(
    OperatorJoinPredicate& join_op_predicate, const std::shared_ptr<AbstractExpression>& join_lqp_predicate,
    const std::shared_ptr<OperatorFeatureNode>& left_input, const std::shared_ptr<OperatorFeatureNode>& right_input,
    const bool operator_flipped_inputs) {
  auto adjusted_lqp_predicate = std::static_pointer_cast<BinaryPredicateExpression>(join_lqp_predicate->deep_copy());
  Assert(adjusted_lqp_predicate,
         "Join predicate is not BinaryPredicateExpression: " + join_lqp_predicate->description());
  // flip PQP predicate if operator flipped it (again)
  if (operator_flipped_inputs) {
    join_op_predicate.flip();
  }

  // flip LQP predicate if PQP predicate is flipped
  if (join_op_predicate.is_flipped()) {
    adjusted_lqp_predicate = std::make_shared<BinaryPredicateExpression>(
        flip_predicate_condition(adjusted_lqp_predicate->predicate_condition), adjusted_lqp_predicate->right_operand(),
        adjusted_lqp_predicate->left_operand());
  }

  // now, LQP and PQP predicate have build input left and probe input right
  Assert(expression_evaluable_on_lqp(adjusted_lqp_predicate->left_operand(), *left_input->get_operator()->lqp_node),
         "Left operand not from left input");
  Assert(expression_evaluable_on_lqp(adjusted_lqp_predicate->right_operand(), *right_input->get_operator()->lqp_node),
         "Right operand not from right input");
  return std::make_shared<PredicateFeatureNode>(adjusted_lqp_predicate, join_op_predicate, left_input, right_input);
}

}  // namespace

namespace hyrise {

OperatorFeatureNode::OperatorFeatureNode(const std::shared_ptr<const AbstractOperator>& op,
                                         const std::shared_ptr<AbstractFeatureNode>& left_input,
                                         const std::shared_ptr<AbstractFeatureNode>& right_input)
    : AbstractFeatureNode{FeatureNodeType::Operator, left_input, right_input},
      _op{op},
      _op_type{map_operator_type(op->type())},
      _run_time{op->performance_data->walltime},
      _output_table{ResultTableFeatureNode::from_operator(op)} {
  Assert(_op_type == QueryOperatorType::GetTable || _left_input, "expected input operator");

  switch (_op_type) {
    case QueryOperatorType::JoinHash:
      _handle_join_hash(static_cast<const JoinHash&>(*_op));
      break;
    case QueryOperatorType::JoinIndex:
      _handle_join_index(static_cast<const JoinIndex&>(*_op));
      break;
    case QueryOperatorType::TableScan:
      _handle_table_scan(static_cast<const TableScan&>(*_op));
      break;
    case QueryOperatorType::IndexScan:
      _handle_index_scan(static_cast<const IndexScan&>(*_op));
      break;
    case QueryOperatorType::Aggregate:
      _handle_aggregate(static_cast<const AggregateHash&>(*_op));
      break;
    case QueryOperatorType::Projection:
      _handle_projection(static_cast<const Projection&>(*_op));
      break;
    case QueryOperatorType::GetTable:
      _handle_get_table(static_cast<const GetTable&>(*_op));
      break;
    default:
      _handle_general_operator(*_op);
      break;
  }
}

std::shared_ptr<OperatorFeatureNode> OperatorFeatureNode::from_pqp(const std::shared_ptr<const AbstractOperator>& op,
                                                                   const std::shared_ptr<Query>& query) {
  auto root_node = _recursively_build_graph_from_pqp(op);
  root_node->set_as_root_node(query);
  return root_node;
}

size_t OperatorFeatureNode::_on_shallow_hash() const {
  const auto& lqp_node = _op->lqp_node;
  Assert(lqp_node, "Operator does not have LQPNode");
  return lqp_node->hash();
}

std::shared_ptr<FeatureVector> OperatorFeatureNode::_on_to_feature_vector() const {
  auto feature_vector = one_hot_encoding<QueryOperatorType>(_op_type);
  const auto& output_feature_vector = _output_table->to_feature_vector();
  feature_vector->insert(feature_vector->end(), output_feature_vector.cbegin(), output_feature_vector.cend());

  feature_vector->reserve(headers().size());

  if (_additional_info) {
    const auto& additional_features = _additional_info->to_feature_vector();
    feature_vector->insert(feature_vector->end(), additional_features.cbegin(), additional_features.cend());
  }

  feature_vector->resize(headers().size());
  return feature_vector;
}

const std::vector<std::string>& OperatorFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& OperatorFeatureNode::headers() {
  static auto ohe_headers_type = one_hot_headers<QueryOperatorType>("operator_type.");
  static const auto output_headers = AbstractTableFeatureNode::headers();
  if (ohe_headers_type.size() == magic_enum::enum_count<QueryOperatorType>()) {
    const auto max_additional_features =
        std::max({TableScanOperatorInfo{}.feature_count(), JoinHashOperatorInfo{}.feature_count()});
    ohe_headers_type.reserve(ohe_headers_type.size() + output_headers.size() + max_additional_features);
    ohe_headers_type.insert(ohe_headers_type.end(), output_headers.cbegin(), output_headers.cend());
    for (auto feature_index = size_t{0}; feature_index < max_additional_features; ++feature_index) {
      ohe_headers_type.emplace_back("additional_feature." + std::to_string(feature_index));
    }
  }
  return ohe_headers_type;
}

std::chrono::nanoseconds OperatorFeatureNode::run_time() const {
  return _run_time;
}

bool OperatorFeatureNode::is_root_node() const {
  return _is_root_node;
}

void OperatorFeatureNode::set_as_root_node(const std::shared_ptr<Query>& query) {
  Assert(!_is_root_node, "Root Node Property should only be set once");
  _is_root_node = true;
  _query = query;
}

std::shared_ptr<Query> OperatorFeatureNode::query() const {
  Assert(_is_root_node, "Only root node is assigned to query");
  return _query;
}

std::shared_ptr<const AbstractOperator> OperatorFeatureNode::get_operator() const {
  return _op;
}

std::shared_ptr<ResultTableFeatureNode> OperatorFeatureNode::output_table() const {
  return _output_table;
}

const std::vector<std::shared_ptr<OperatorFeatureNode>>& OperatorFeatureNode::subqueries() const {
  return _subqueries;
}

void OperatorFeatureNode::_handle_general_operator(const AbstractOperator& op) {}

void OperatorFeatureNode::_handle_join_hash(const JoinHash& join_hash) {
  Assert(_right_input && _left_input->type() == FeatureNodeType::Operator &&
             _right_input->type() == FeatureNodeType::Operator,
         "Join needs two input operators");

  const auto& performance_data = static_cast<JoinHash::PerformanceData&>(*_op->performance_data);
  // for us, the right input is ALWAYS the probe column and the left input is the build side
  auto flipped_inputs = !performance_data.left_input_is_build_side;
  if (flipped_inputs) {
    std::swap(_left_input, _right_input);
  }

  const auto left_input_node = std::static_pointer_cast<OperatorFeatureNode>(_left_input);
  const auto right_input_node = std::static_pointer_cast<OperatorFeatureNode>(_right_input);

  auto join_op_predicates = std::vector<OperatorJoinPredicate>{join_hash.primary_predicate()};
  const auto& secondary_predicates = join_hash.secondary_predicates();
  join_op_predicates.insert(join_op_predicates.end(), secondary_predicates.cbegin(), secondary_predicates.cend());
  const auto& join_node = static_cast<const JoinNode&>(*join_hash.lqp_node);
  const auto& join_lqp_predicates = join_node.join_predicates();
  const auto join_predicate_count = join_op_predicates.size();
  Assert(join_predicate_count == join_lqp_predicates.size(), "LQP and PQP predicates do not match");
  for (auto predicate_id = ColumnID{0}; predicate_id < join_predicate_count; ++predicate_id) {
    _expressions.emplace_back(resolve_join_predicate(join_op_predicates.at(predicate_id),
                                                     join_lqp_predicates.at(predicate_id), left_input_node,
                                                     right_input_node, flipped_inputs));
  }

  _additional_info = std::make_unique<JoinHashOperatorInfo>();
  auto& join_additional_info = static_cast<JoinHashOperatorInfo&>(*_additional_info);
  join_additional_info.radix_bits = performance_data.radix_bits;
  join_additional_info.build_side_materialized_value_count = performance_data.build_side_materialized_value_count;
  join_additional_info.probe_side_materialized_value_count = performance_data.probe_side_materialized_value_count;
  join_additional_info.hash_tables_distinct_value_count = performance_data.hash_tables_distinct_value_count;
  join_additional_info.hash_tables_position_count = performance_data.hash_tables_position_count.value_or(0);
}

void OperatorFeatureNode::_handle_join_index(const JoinIndex& join_index) {
  Assert(_right_input, "Join needs two inputs");

  // for us, the right input is ALWAYS the index column
  const auto& performance_data = static_cast<JoinIndex::PerformanceData&>(*_op->performance_data);
  // for us, the right input is ALWAYS the probe column and the left input is the build side
  auto flipped_inputs = !performance_data.right_input_has_index;
  if (flipped_inputs) {
    std::swap(_left_input, _right_input);
  }

  const auto left_input_node = std::static_pointer_cast<OperatorFeatureNode>(_left_input);
  const auto right_input_node = std::static_pointer_cast<OperatorFeatureNode>(_right_input);

  auto join_op_predicate = join_index.primary_predicate();
  Assert(join_index.secondary_predicates().empty(), "Index Joinx with secondary predicates found");
  const auto& join_node = static_cast<const JoinNode&>(*join_index.lqp_node);
  const auto& join_lqp_predicate = *join_node.join_predicates().cbegin();
  _expressions.emplace_back(
      resolve_join_predicate(join_op_predicate, join_lqp_predicate, left_input_node, right_input_node, flipped_inputs));
}

void OperatorFeatureNode::_handle_table_scan(const TableScan& table_scan) {
  const auto& predicate_node = static_cast<const PredicateNode&>(*table_scan.lqp_node);
  _expressions.emplace_back(
      std::make_shared<PredicateFeatureNode>(predicate_node.predicate(), table_scan.predicate(), _left_input));
  _add_subqueries(table_scan.predicate()->arguments);

  const auto& performance_data = static_cast<TableScan::PerformanceData&>(*table_scan.performance_data);
  _additional_info = std::make_unique<TableScanOperatorInfo>();

  auto& scan_additional_info = static_cast<TableScanOperatorInfo&>(*_additional_info);
  scan_additional_info.skipped_chunks_none_match = performance_data.num_chunks_with_early_out;
  scan_additional_info.skipped_chunks_all_match = performance_data.num_chunks_with_all_rows_matching;
  scan_additional_info.skipped_chunks_binary_search = performance_data.num_chunks_with_binary_search;
}

void OperatorFeatureNode::_handle_index_scan(const IndexScan& index_scan) {
  // TODO
}

void OperatorFeatureNode::_handle_aggregate(const AggregateHash& aggregate) {
  const auto& aggregate_node = static_cast<const AggregateNode&>(*aggregate.lqp_node);
  const auto& groupby_column_ids = aggregate.groupby_column_ids();
  const auto groupby_count = groupby_column_ids.size();
  const auto lqp_expressions = aggregate_node.node_expressions;
  Assert(groupby_count == aggregate_node.aggregate_expressions_begin_idx, "Mismatching group by columns");
  for (auto expression_id = ColumnID{0}; expression_id < groupby_count; ++expression_id) {
    _expressions.emplace_back(ColumnFeatureNode::from_expression(_left_input, lqp_expressions[expression_id],
                                                                 groupby_column_ids[expression_id]));
  }

  const auto& aggregate_expressions = aggregate.aggregates();
  const auto aggregate_count = aggregate_expressions.size();
  Assert(groupby_count + aggregate_count == lqp_expressions.size(), "Expressions not equal");
  for (auto expression_id = ColumnID{0}; expression_id < aggregate_count; ++expression_id) {
    _expressions.emplace_back(std::make_shared<AggregateFunctionFeatureNode>(
        lqp_expressions[groupby_count + expression_id], aggregate_expressions[expression_id], _left_input));
  }

  _additional_info = std::make_unique<AggregateHashOperatorInfo>();
  auto& aggregate_additional_info = static_cast<AggregateHashOperatorInfo&>(*_additional_info);
  aggregate_additional_info.groupby_column_count = groupby_count;
  aggregate_additional_info.aggregate_column_count = aggregate_count;
}

void OperatorFeatureNode::_handle_projection(const Projection& projection) {
  const auto& expressions = projection.expressions;

  // set mterialized columns if table is not data table
  // First gather all forwardable PQP column expressions.
  auto forwarded_pqp_columns = ExpressionUnorderedSet{};
  for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
    const auto& expression = expressions[column_id];
    if (expression->type == ExpressionType::PQPColumn) {
      forwarded_pqp_columns.emplace(expression);
    }
  }

  // Iterate the expressions and check if a forwarded column is part of an expression. In this case, remove it from
  // the list of forwarded columns. When the input is a data table (and thus the output table is as well) the
  // forwarded column does not need to be accessed via its position list later. And since the following operator might
  // have optimizations for accessing an encoded segment, we always forward for data tables.
  if (_output_table->table_type() == TableType::References) {
    for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
      const auto& expression = expressions[column_id];

      if (expression->type == ExpressionType::PQPColumn) {
        continue;
      }

      visit_expression(expression, [&](const auto& sub_expression) {
        if (sub_expression->type == ExpressionType::PQPColumn) {
          forwarded_pqp_columns.erase(sub_expression);
        }
        return ExpressionVisitation::VisitArguments;
      });
    }
  }

  const auto& projection_node = static_cast<const ProjectionNode&>(*projection.lqp_node);
  const auto& output_expressions = projection_node.output_expressions();
  for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
    const auto& expression = expressions[column_id];
    if (!forwarded_pqp_columns.contains(expression)) {
      _output_table->set_column_materialized(column_id);
      _expressions.emplace_back(
          std::make_shared<PredicateFeatureNode>(output_expressions.at(column_id), expression, _left_input));
    }
  }
}

void OperatorFeatureNode::_handle_get_table(const GetTable& get_table) {
  const auto& table_name = get_table.table_name();
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  _left_input = BaseTableFeatureNode::from_table(table, table_name);
}

void OperatorFeatureNode::_add_subqueries(const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  for (const auto& expression : expressions) {
    if (auto subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(expression)) {
      if (!subquery_expression->is_correlated()) {
        _subqueries.emplace_back(_recursively_build_graph_from_pqp(subquery_expression->pqp));
      }
    }
  }
}

const std::vector<std::shared_ptr<AbstractFeatureNode>>& OperatorFeatureNode::expressions() const {
  return _expressions;
}

// Additional Operator Info
// TableScan Info
const std::vector<std::string>& OperatorFeatureNode::TableScanOperatorInfo::headers() {
  static auto headers =
      std::vector<std::string>{"table_scan.skipped_chunks_none_match", "table_scan.skipped_chunks_all_match",
                               "table_scan.skipped_chunks_binary_search"};
  return headers;
}

const FeatureVector OperatorFeatureNode::TableScanOperatorInfo::to_feature_vector() const {
  return FeatureVector{Feature{static_cast<Feature>(skipped_chunks_none_match)},
                       Feature{static_cast<Feature>(skipped_chunks_all_match)},
                       Feature{static_cast<Feature>(skipped_chunks_binary_search)}};
}

size_t OperatorFeatureNode::TableScanOperatorInfo::feature_count() const {
  return headers().size();
}

// JoinHash Info
const std::vector<std::string>& OperatorFeatureNode::JoinHashOperatorInfo::headers() {
  static auto headers = std::vector<std::string>{"join_hash.radix_bits",
                                                 "join_hash.build_side_materialized_value_count",
                                                 "join_hash.build_side_materialized_value_count",
                                                 "join_hash.probe_side_materialized_value_count",
                                                 "join_hash.hash_tables_distinct_value_count",
                                                 "join_hash.hash_tables_position_count"};
  return headers;
}

const FeatureVector OperatorFeatureNode::JoinHashOperatorInfo::to_feature_vector() const {
  return FeatureVector{Feature{static_cast<Feature>(radix_bits)},
                       Feature{static_cast<Feature>(build_side_materialized_value_count)},
                       Feature{static_cast<Feature>(probe_side_materialized_value_count)},
                       Feature{static_cast<Feature>(hash_tables_distinct_value_count)},
                       Feature{static_cast<Feature>(hash_tables_position_count)}};
}

size_t OperatorFeatureNode::JoinHashOperatorInfo::feature_count() const {
  return headers().size();
}

// AggregateHash Info
const std::vector<std::string>& OperatorFeatureNode::AggregateHashOperatorInfo::headers() {
  static auto headers =
      std::vector<std::string>{"aggregate_hash.groupby_column_count", "aggregate_hash.aggregate_column_count"};
  return headers;
}

const FeatureVector OperatorFeatureNode::AggregateHashOperatorInfo::to_feature_vector() const {
  return FeatureVector{Feature{static_cast<Feature>(groupby_column_count)},
                       Feature{static_cast<Feature>(aggregate_column_count)}};
}

size_t OperatorFeatureNode::AggregateHashOperatorInfo::feature_count() const {
  return headers().size();
}

}  // namespace hyrise
