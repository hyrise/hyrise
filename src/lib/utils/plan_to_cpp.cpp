#include "plan_to_cpp.hpp"

#include <sstream>
#include <iomanip>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/create_table_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "constant_mappings.hpp"

namespace {

using namespace opossum; // NOLINT

void print_lqp(const std::shared_ptr<AbstractLQPNode>& node, int indent, std::stringstream& cpp_stream, const ColumnExpressionNames& column_expressions_by_name, const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::string>& referenced_node_names) {
  cpp_stream << std::setw(indent * 2) << " " << std::setw(0);

  switch (node->type) {
    case LQPNodeType::Aggregate: {
      const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);
      cpp_stream << "AggregateNode::make(expression_vector(";
      for (auto column_id = ColumnID{0}; column_id < aggregate_node->aggregate_expressions_begin_idx; ++column_id) {
        cpp_stream << expression_to_cpp(aggregate_node->node_expressions[column_id], column_expressions_by_name);
        if (column_id + 1 < aggregate_node->aggregate_expressions_begin_idx) cpp_stream << ", ";
      }
      cpp_stream << "), expression_vector(";
      for (auto column_id = aggregate_node->aggregate_expressions_begin_idx; column_id < aggregate_node->node_expressions.size(); ++column_id) {
        cpp_stream << expression_to_cpp(aggregate_node->node_expressions[column_id], column_expressions_by_name);
        if (column_id + 1 < aggregate_node->node_expressions.size()) cpp_stream << ", ";
      }
      cpp_stream << "),";

    } break;
    case LQPNodeType::Alias: {
      const auto alias_node = std::dynamic_pointer_cast<AliasNode>(node);
      cpp_stream << "AliasNode::make(expression_vector(";
      for (auto column_id = ColumnID{0}; column_id < alias_node->node_expressions.size(); ++column_id) {
        cpp_stream << expression_to_cpp(alias_node->node_expressions[column_id], column_expressions_by_name);
        if (column_id + 1 < alias_node->node_expressions.size()) cpp_stream << ", ";
      }
      cpp_stream << "), std::vector<std::string>{";
      for (auto column_id = ColumnID{0}; column_id < alias_node->aliases.size(); ++column_id) {
        cpp_stream << alias_node->aliases[column_id];
        if (column_id + 1 < alias_node->aliases.size()) cpp_stream << ", ";
      }
      cpp_stream << "},";

    } break;
    case LQPNodeType::CreateTable: {


    } break;
    case LQPNodeType::CreatePreparedPlan: {
      //const auto concrete_node = std::dynamic_pointer_cast<CreatePreparedPlanNode>(node);

    } break;
    case LQPNodeType::CreateView: {
      //const auto concrete_node = std::dynamic_pointer_cast<CreateViewNode>(node);

    } break;
    case LQPNodeType::Delete: {
      //const auto concrete_node = std::dynamic_pointer_cast<DeleteNode>(node);

    } break;
    case LQPNodeType::DropView: {
      //const auto concrete_node = std::dynamic_pointer_cast<DropViewNode>(node);

    } break;
    case LQPNodeType::DropTable: {
      //const auto concrete_node = std::dynamic_pointer_cast<DropTableNode>(node);

    } break;
    case LQPNodeType::DummyTable: {
      //const auto concrete_node = std::dynamic_pointer_cast<DummyTableNode>(node);

    } break;
    case LQPNodeType::Insert: {
      //const auto concrete_node = std::dynamic_pointer_cast<InsertNode>(node);

    } break;
    case LQPNodeType::Join: {
      const auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
      cpp_stream << "JoinNode::make(JoinMode::" << join_mode_to_string.at(join_node->join_mode);

      if (join_node->join_mode != JoinMode::Cross) {
        cpp_stream << ", " << expression_to_cpp(join_node->join_predicate(), column_expressions_by_name);
      }
      cpp_stream << ",";

    } break;
    case LQPNodeType::Limit: {
      //const auto concrete_node = std::dynamic_pointer_cast<LimitNode>(node);

    } break;
    case LQPNodeType::Predicate: {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
      cpp_stream << "PredicateNode::make(" << expression_to_cpp(predicate_node->predicate(), column_expressions_by_name);
      cpp_stream << ",";

    } break;
    case LQPNodeType::Projection: {
      //const auto concrete_node = std::dynamic_pointer_cast<ProjectionNode>(node);

    } break;
    case LQPNodeType::Root: {
      //const auto concrete_node = std::dynamic_pointer_cast<RootNode>(node);

    } break;
    case LQPNodeType::ShowColumns: {
      //const auto concrete_node = std::dynamic_pointer_cast<ShowColumnsNode>(node);

    } break;
    case LQPNodeType::ShowTables: {
      //const auto concrete_node = std::dynamic_pointer_cast<ShowTablesNode>(node);

    } break;
    case LQPNodeType::Sort: {
      const auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
      cpp_stream << "SortNode::make(expression_vector(";

      for (auto column_id = ColumnID{0}; column_id < sort_node->node_expressions.size(); ++column_id) {
        cpp_stream << expression_to_cpp(sort_node->node_expressions[column_id], column_expressions_by_name);
        if (column_id + 1 < sort_node->node_expressions.size()) cpp_stream << ", ";
      }
      cpp_stream << "), std::vector<OrderByMode>{";
      for (auto column_id = ColumnID{0}; column_id < sort_node->order_by_modes.size(); ++column_id) {
        cpp_stream << "OrderByMode::" << order_by_mode_to_string.at(sort_node->order_by_modes[column_id]);
        if (column_id + 1 < sort_node->order_by_modes.size()) cpp_stream << ", ";
      }
      cpp_stream << "},";


    } break;
    case LQPNodeType::StoredTable: {
      const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
      cpp_stream << referenced_node_names.at(stored_table_node);
      return; // No closing parentheses nor inputs

    } break;
    case LQPNodeType::Update: {
      //const auto concrete_node = std::dynamic_pointer_cast<UpdateNode>(node);

    } break;
    case LQPNodeType::Union: {
      //const auto concrete_node = std::dynamic_pointer_cast<UnionNode>(node);

    } break;
    case LQPNodeType::Validate: {
      //const auto concrete_node = std::dynamic_pointer_cast<ValidateNode>(node);
      cpp_stream << "ValidateNode::make(";
    } break;
    case LQPNodeType::Mock: {
      //const auto concrete_node = std::dynamic_pointer_cast<MockNode>(node);

    } break;
  }

  if (node->input_count() != 0) {
    cpp_stream << std::endl;
  }

  if (node->left_input()) {
    print_lqp(node->left_input(), indent + 1, cpp_stream, column_expressions_by_name, referenced_node_names);
    if (node->right_input()) {
      cpp_stream << ", " << std::endl;
      print_lqp(node->right_input(), indent + 1, cpp_stream, column_expressions_by_name, referenced_node_names);
    }
  }

  cpp_stream << ")";
}

}

namespace opossum {

std::string expression_to_cpp(const std::shared_ptr<AbstractExpression>& expression, const ColumnExpressionNames& column_expression_names) {
  std::stringstream cpp_stream;

  switch (expression->type) {
    case ExpressionType::Aggregate: {
      //const auto concrete_expression = std::dynamic_pointer_cast<AggregateExpression>(expression);
    } break;

    case ExpressionType::Arithmetic: {
      //const auto concrete_expression = std::dynamic_pointer_cast<ArithmeticExpression>(expression);
    } break;

    case ExpressionType::Cast: {
      //const auto concrete_expression = std::dynamic_pointer_cast<CastExpression>(expression);
    } break;

    case ExpressionType::Case: {
      //const auto concrete_expression = std::dynamic_pointer_cast<CaseExpression>(expression);
    } break;

    case ExpressionType::CorrelatedParameter: {
      //const auto concrete_expression = std::dynamic_pointer_cast<CorrelatedParameterExpression>(expression);
    } break;

    case ExpressionType::PQPColumn: {
      //const auto concrete_expression = std::dynamic_pointer_cast<PQPColumnExpression>(expression);
    } break;

    case ExpressionType::LQPColumn: {
      const auto lqp_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      cpp_stream << column_expression_names.at(lqp_column_expression);
    } break;

    case ExpressionType::Exists: {
      //const auto concrete_expression = std::dynamic_pointer_cast<ExistsExpression>(expression);
    } break;

    case ExpressionType::Extract: {
      //const auto concrete_expression = std::dynamic_pointer_cast<ExtractExpression>(expression);
    } break;

    case ExpressionType::Function: {
      //const auto concrete_expression = std::dynamic_pointer_cast<FunctionExpression>(expression);
    } break;

    case ExpressionType::List: {
      //const auto concrete_expression = std::dynamic_pointer_cast<ListExpression>(expression);
    } break;

    case ExpressionType::Logical: {
      //const auto concrete_expression = std::dynamic_pointer_cast<LogicalExpression>(expression);
    } break;

    case ExpressionType::Placeholder: {
      //const auto concrete_expression = std::dynamic_pointer_cast<PlaceholderExpression>(expression);
    } break;

    case ExpressionType::Predicate: {
      const auto predicate_expression = std::dynamic_pointer_cast<AbstractPredicateExpression>(expression);
      switch (predicate_expression->predicate_condition) {
        case PredicateCondition::Equals: cpp_stream << "equals_"; break;
        case PredicateCondition::NotEquals: cpp_stream << "not_equals_"; break;
        case PredicateCondition::LessThan: cpp_stream << "less_than_"; break;
        case PredicateCondition::LessThanEquals: cpp_stream << "less_than_equals_"; break;
        case PredicateCondition::GreaterThan: cpp_stream << "greater_than_"; break;
        case PredicateCondition::GreaterThanEquals: cpp_stream << "greater_than_equals_"; break;
        case PredicateCondition::Between: cpp_stream << "between_"; break;
        case PredicateCondition::In: cpp_stream << "in_"; break;
        case PredicateCondition::NotIn: cpp_stream << "not_in_"; break;
        case PredicateCondition::Like: cpp_stream << "like_"; break;
        case PredicateCondition::NotLike: cpp_stream << "not_like_"; break;
        case PredicateCondition::IsNull: cpp_stream << "is_not_null_"; break;
        case PredicateCondition::IsNotNull: cpp_stream << "is_not_null_"; break;
      }
    } break;

    case ExpressionType::PQPSelect: {
      //const auto concrete_expression = std::dynamic_pointer_cast<PQPSelectExpression>(expression);
    } break;

    case ExpressionType::LQPSelect: {
      //const auto concrete_expression = std::dynamic_pointer_cast<LQPSelectExpression>(expression);
    } break;

    case ExpressionType::UnaryMinus: {
      cpp_stream << "unary_minus_";
    } break;

    case ExpressionType::Value: {
      const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(expression);
      if (value_expression->value.type() == typeid(std::string)) {
        cpp_stream << "\"" << value_expression->value << "\"";
      } else {
        cpp_stream << value_expression->value;
      }
    } break;
  }

  if (!expression->arguments.empty()) {
    cpp_stream << "(";
    for (const auto &argument : expression->arguments) {
      cpp_stream << expression_to_cpp(argument, column_expression_names);
      if (argument != expression->arguments.back()) {
        cpp_stream << ", ";
      }
    }
    cpp_stream << ")";
  }

  return cpp_stream.str();
}

std::string lqp_to_cpp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  ColumnExpressionNames column_expression_names;

  // If multiple columns have the same name, this is to suffix them with an integer
  std::unordered_map<std::string, size_t> column_name_counts;

  std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::string> referenced_node_names;

  visit_lqp(lqp, [&](const auto& node) {
    //std::cout << "Node: " << node->description() << std::endl;
    for (const auto& node_expression : node->node_expressions) {
      //std::cout << "  NodeExpression: " << node_expression->as_column_name() << std::endl;
      visit_expression(node_expression, [&](const auto& sub_expression) {
        if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
          //std::cout << "   ColumnExpression: " << column_expression->as_column_name() << std::endl;
          if (column_expression_names.count(column_expression) == 0) {
            auto column_name = column_expression->as_column_name();
            const auto column_name_count = ++column_name_counts[column_name];

            if (column_name_count > 1u) {
              // TODO(moritz) This could still create a non-unique name, e.g. if there is a column "a_2" for some reason
              //              Unlikely, but might happen
              column_name += "_" + std::to_string(column_name_count + 1);
            }

            column_expression_names.emplace(column_expression, column_name);
          }

          return ExpressionVisitation::DoNotVisitArguments;
        } else {
          return ExpressionVisitation::VisitArguments;
        }
      });
    }

    if (const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node)) {
      referenced_node_names.emplace(stored_table_node, stored_table_node->table_name);
    }

    return LQPVisitation::VisitInputs;
  });

  std::stringstream cpp_stream;


  for (const auto& [referenced_node, referenced_node_name] : referenced_node_names) {
    if (const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(referenced_node)) {
      cpp_stream << "const auto " << referenced_node_name << " = StoredTableNode::make(\"" << stored_table_node->table_name << "\");" << std::endl;
    }
  }
  cpp_stream << std::endl;

  for (const auto& [column_expression, column_expression_name] : column_expression_names) {
    const auto stored_table_node_name = referenced_node_names.at(column_expression->column_reference.original_node());
    cpp_stream << "const auto " << column_expression_name << " = lqp_column_(" << stored_table_node_name << ", ColumnID{" << column_expression->column_reference.original_column_id() << "})" << std::endl;
  }
  cpp_stream << std::endl;

  cpp_stream << "const auto lqp = " << std::endl;

  print_lqp(lqp, 0u, cpp_stream, column_expression_names, referenced_node_names);

  cpp_stream << ";";

  return cpp_stream.str();
}

}  // namespace opossum