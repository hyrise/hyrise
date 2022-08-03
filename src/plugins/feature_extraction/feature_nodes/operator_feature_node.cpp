#include "operator_feature_node.hpp"

#include "expression/pqp_subquery_expression.hpp"
#include "feature_extraction/feature_nodes/base_table_feature_node.hpp"
#include "feature_extraction/feature_nodes/predicate_feature_node.hpp"
#include "feature_extraction/util/feature_extraction_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/predicate_node.hpp"

namespace {

using namespace opossum;

std::shared_ptr<OperatorFeatureNode> _recursively_build_graph_from_pqp(
    const std::shared_ptr<const AbstractOperator>& op) {
  const auto left_input = op->left_input() ? _recursively_build_graph_from_pqp(op->left_input()) : nullptr;
  const auto right_input = op->right_input() ? _recursively_build_graph_from_pqp(op->right_input()) : nullptr;

  return std::make_shared<OperatorFeatureNode>(op, left_input, right_input);
}

}  // namespace

namespace opossum {

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
  /*size_t hash{0};

  for (const auto& predicate : _predicates) {
    boost::hash_combine(hash, predicate->type());
    boost::hash_combine(hash, predicate->hash());
  }

  boost::hash_combine(hash, _op_type);

  return hash;*/
}

std::shared_ptr<FeatureVector> OperatorFeatureNode::_on_to_feature_vector() const {
  auto feature_vector = one_hot_encoding<QueryOperatorType>(_op_type);
  const auto& output_feature_vector = _output_table->to_feature_vector();
  feature_vector->insert(feature_vector->end(), output_feature_vector.begin(), output_feature_vector.end());
  return feature_vector;
}

const std::vector<std::string>& OperatorFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& OperatorFeatureNode::headers() {
  static auto ohe_headers_type = one_hot_headers<QueryOperatorType>("operator_type.");
  static const auto output_headers = AbstractTableFeatureNode::headers();
  if (ohe_headers_type.size() == magic_enum::enum_count<QueryOperatorType>()) {
    ohe_headers_type.insert(ohe_headers_type.end(), output_headers.begin(), output_headers.end());
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

const std::vector<std::shared_ptr<AbstractFeatureNode>>& OperatorFeatureNode::subqueries() const {
  return _subqueries;
}

void OperatorFeatureNode::_handle_general_operator(const AbstractOperator& op) {}

void OperatorFeatureNode::_handle_join_hash(const JoinHash& join_hash) {
  Assert(_right_input, "Join needs two inputs");

  const auto& performance_data = static_cast<JoinHash::PerformanceData&>(_op->performance_data);
  // for us, the right input is ALWAYS the probe column and the left input is the build side
  auto flipped_inputs = !performance_data.left_input_is_build_side;
  if (flipped_inputs) {
    std::swap(_left_input, _right_input);
  }

  // if the operator flipped the sides, all predicates were flipped as well
  // however, they could have been flipped on instantiation, and we don't know which sides of the
  // LQP node's predicates belong to them
  // need to handle that for the predicates...


}
void OperatorFeatureNode::_handle_join_index(const JoinIndex& join_index) {
  Assert(_right_input, "Join needs two inputs");

  // for us, the right input is ALWAYS the index column
  const auto& performance_data = static_cast<JoinIndex::PerformanceData&>(_op->performance_data);
  // for us, the right input is ALWAYS the probe column and the left input is the build side
  auto flipped_inputs = !performance_data.right_input_has_index;
  if (flipped_inputs) {
    std::swap(_left_input, _right_input);
  }
}

void OperatorFeatureNode::_handle_table_scan(const TableScan& table_scan) {
  const auto& predicate_node = static_cast<const PredicateNode&>(*table_scan.lqp_node);
  _predicates.push_back(
      std::make_shared<PredicateFeatureNode>(predicate_node.predicate(), table_scan.predicate(), _left_input));
  _add_subqueries(table_scan.predicate()->arguments);
}

void OperatorFeatureNode::_handle_index_scan(const IndexScan& index_scan) {}

void OperatorFeatureNode::_handle_aggregate(const AggregateHash& aggregate) {}

void OperatorFeatureNode::_handle_projection(const Projection& projection) {
  const auto& expressions = projection.expressions;

  // set mterialized columns if table is not data table
  // First gather all forwardable PQP column expressions.
  auto forwarded_pqp_columns = ExpressionUnorderedSet{};
  for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
    const auto& expression = expressions[column_id];
    if (expression->type == ExpressionType::PQPColumn) {
      forwarded_pqp_columns.emplace(expression_id);
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

  const auto& projection_node = static_cast<ProjectionNode&>(projection.lqp_node);
  const auto& output_expressions = projection_node.output_expressions();
  for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
    const auto& expression = expressions[column_id];
    if (!forwarded_pqp_columns.contains(expression)) {
      _output_table->set_column_materialized(column_id);
      _predicates.push_back(std::make_shared<PredicateFeatureNode>(output_expressions.at(column_id)), expression,
                            _left_input)
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
        _subqueries.push_back(_recursively_build_graph_from_pqp(subquery_expression->pqp));
      }
    }
  }
}

const std::vector<std::shared_ptr<AbstractFeatureNode>>& OperatorFeatureNode::predicates() const {
  return _predicates;
}

}  // namespace opossum
