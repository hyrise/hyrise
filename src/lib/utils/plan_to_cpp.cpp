#include "plan_to_cpp.hpp"

#include <sstream>
#include <iomanip>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/create_table_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "constant_mappings.hpp"

using namespace std::string_literals; // NOLINT

namespace {

using namespace opossum; // NOLINT

struct State {
  std::ostringstream stream;

  std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, std::string>> subplans_names;
  size_t subplan_count{0};
};

std::string generate_unique_name(State& state, const std::string& hint) {
  return hint;
}

std::string print_expression_vector(const std::vector<std::shared_ptr<AbstractExpression>>& expressions, const ColumnExpressionNames& column_expressions_by_name) {
  std::ostringstream stream;
  stream << "expression_vector(";
  for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
    stream << expression_to_cpp(expressions[column_id], column_expressions_by_name);
    if (column_id + 1 < expressions.size()) stream << ", ";
  }
  stream << ")";
  return stream.str();
}

void print_lqp(const std::shared_ptr<AbstractLQPNode>& node, int indent, std::ostringstream& stream, const ColumnExpressionNames& column_expressions_by_name, const std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::string>& referenced_node_names) {
  stream << std::setw(indent * 2) << " " << std::setw(0);

  switch (node->type) {
    case LQPNodeType::Aggregate: {
      const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);
      stream << "AggregateNode::make(expression_vector(";
      for (auto column_id = ColumnID{0}; column_id < aggregate_node->aggregate_expressions_begin_idx; ++column_id) {
        stream << expression_to_cpp(aggregate_node->node_expressions[column_id], column_expressions_by_name);
        if (column_id + 1 < aggregate_node->aggregate_expressions_begin_idx) stream << ", ";
      }
      stream << "), expression_vector(";
      for (auto column_id = aggregate_node->aggregate_expressions_begin_idx; column_id < aggregate_node->node_expressions.size(); ++column_id) {
        stream << expression_to_cpp(aggregate_node->node_expressions[column_id], column_expressions_by_name);
        if (column_id + 1 < aggregate_node->node_expressions.size()) stream << ", ";
      }
      stream << "),";

    } break;

    case LQPNodeType::Alias: {
      const auto alias_node = std::dynamic_pointer_cast<AliasNode>(node);
      stream << "AliasNode::make(";
      stream << print_expression_vector(alias_node->node_expressions, column_expressions_by_name);
      stream << ", std::vector<std::string>{";
      for (auto column_id = ColumnID{0}; column_id < alias_node->aliases.size(); ++column_id) {
        stream << alias_node->aliases[column_id];
        if (column_id + 1 < alias_node->aliases.size()) stream << ", ";
      }
      stream << "},";

    } break;

    case LQPNodeType::CreateTable: {
      const auto create_table_node = std::dynamic_pointer_cast<CreateTableNode>(node);
      stream << "CreateTable::make(" << create_table_node->table_name << ", TableColumnDefinitions{";

      for (auto column_id = ColumnID{0}; column_id < create_table_node->column_definitions.size(); ++column_id) {
        const auto& column_definition = create_table_node->column_definitions[column_id];
        stream << "{\"" << column_definition.name << "\", DataType::" << data_type_to_string.left.at(column_definition.data_type) << ", " << (column_definition.nullable ? "true" : "false") << "}";
        if (column_id + 1 < create_table_node->column_definitions.size()) {
          stream << ", ";
        }
      }

      stream << "})";

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
      stream << "JoinNode::make(JoinMode::" << join_mode_to_string.at(join_node->join_mode);

      if (join_node->join_mode != JoinMode::Cross) {
        stream << ", " << expression_to_cpp(join_node->join_predicate(), column_expressions_by_name);
      }
      stream << ",";

    } break;
    case LQPNodeType::Limit: {
      //const auto concrete_node = std::dynamic_pointer_cast<LimitNode>(node);

    } break;
    case LQPNodeType::Predicate: {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
      stream << "PredicateNode::make(" << expression_to_cpp(predicate_node->predicate(), column_expressions_by_name);
      stream << ",";

    } break;
    case LQPNodeType::Projection: {
      const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node);

      stream << "ProjectionNode::make(expression_vector(" << print_expression_vector(projection_node->node_expressions, column_expressions_by_name);
      stream << ",";

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
      stream << "SortNode::make(expression_vector(";

      for (auto column_id = ColumnID{0}; column_id < sort_node->node_expressions.size(); ++column_id) {
        stream << expression_to_cpp(sort_node->node_expressions[column_id], column_expressions_by_name);
        if (column_id + 1 < sort_node->node_expressions.size()) stream << ", ";
      }
      stream << "), std::vector<OrderByMode>{";
      for (auto column_id = ColumnID{0}; column_id < sort_node->order_by_modes.size(); ++column_id) {
        stream << "OrderByMode::" << order_by_mode_to_string.at(sort_node->order_by_modes[column_id]);
        if (column_id + 1 < sort_node->order_by_modes.size()) stream << ", ";
      }
      stream << "},";


    } break;
    case LQPNodeType::StoredTable: {
      const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
      stream << referenced_node_names.at(stored_table_node);
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
      stream << "ValidateNode::make(";
    } break;
    case LQPNodeType::Mock: {
      //const auto concrete_node = std::dynamic_pointer_cast<MockNode>(node);

    } break;
  }

  if (node->input_count() != 0) {
    stream << std::endl;
  }

  if (node->left_input()) {
    print_lqp(node->left_input(), indent + 1, stream, column_expressions_by_name, referenced_node_names);
    if (node->right_input()) {
      stream << ", " << std::endl;
      print_lqp(node->right_input(), indent + 1, stream, column_expressions_by_name, referenced_node_names);
    }
  }

  stream << ")";
}

void lqp_to_cpp_impl(const std::shared_ptr<AbstractLQPNode>& lqp, const std::string& name, State& state) {
  ColumnExpressionNames column_expression_names;

  // If multiple columns have the same name, this is to suffix them with an integer
  std::unordered_map<std::string, size_t> column_name_counts;

  std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::string> referenced_node_names;

  visit_lqp(lqp, [&](const auto& node) {
    for (const auto& node_expression : node->node_expressions) {
      visit_expression(node_expression, [&](const auto& sub_expression) {
        if (const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(sub_expression)) {
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
        } else if (const auto select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(sub_expression)) {
          auto subplan_name_iter = std::find_if(state.subplans_names.begin(), state.subplans_names.end(), [&](const auto& plan_and_name) {
            return *plan_and_name.first == *select_expression->lqp;
          });

          if (subplan_name_iter == state.subplans_names.end()) {
            const auto subplan_name = generate_unique_name(state, "subplan_"s + std::to_string(state.subplan_count++));
            lqp_to_cpp_impl(select_expression->lqp, subplan_name, state);
            state.subplans_names.emplace_back(select_expression->lqp, subplan_name);
          }

          return ExpressionVisitation::VisitArguments;
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



  for (const auto& [referenced_node, referenced_node_name] : referenced_node_names) {
    if (const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(referenced_node)) {
      state.stream << "const auto " << referenced_node_name << " = StoredTableNode::make(\"" << stored_table_node->table_name << "\");" << std::endl;
    }
  }
  state.stream << std::endl;

  for (const auto& [column_expression, column_expression_name] : column_expression_names) {
    const auto stored_table_node_name = referenced_node_names.at(column_expression->column_reference.original_node());
    state.stream << "const auto " << column_expression_name << " = lqp_column_({" << stored_table_node_name << ", ColumnID{" << column_expression->column_reference.original_column_id() << "}});" << std::endl;
  }
  state.stream << std::endl;

  state.stream << "const auto " << name << " = " << std::endl;

  print_lqp(lqp, 0u, state.stream, column_expression_names, referenced_node_names);

  state.stream << ";" << std::endl;
}

}

namespace opossum {

std::string expression_to_cpp(const std::shared_ptr<AbstractExpression>& expression, const ColumnExpressionNames& column_expression_names) {
  std::stringstream stream;

  switch (expression->type) {
    case ExpressionType::Aggregate: {
      const auto aggregate_expression = std::dynamic_pointer_cast<AggregatExpression>(expression);
      switch (aggregate_expression->aggregate_function) {
        case AggregateFunction::Min: stream << "min_";break;
        case AggregateFunction::Max:stream << "max_";break;
        case AggregateFunction::Sum:stream << "sum_";break;
        case AggregateFunction::Avg:stream << "avg_";break;
        case AggregateFunction::Count:stream << "count_";break;
        case AggregateFunction::CountDistinct:stream << "count_distinct_"; break;
      }

      stream << aggregate_function_to_string.left.at();
    } break;

    case ExpressionType::Arithmetic: {
      const auto arithmetic_expression = std::dynamic_pointer_cast<ArithmeticExpression>(expression);
      switch (arithmetic_expression->arithmetic_operator) {
        case ArithmeticOperator::Addition: stream << "add_"; break;
        case ArithmeticOperator::Subtraction: stream << "sub_"; break;
        case ArithmeticOperator::Multiplication: stream << "mul_"; break;
        case ArithmeticOperator::Division: stream << "div_"; break;
        case ArithmeticOperator::Modulo: stream << "mod_"; break;
      }

    } break;

    case ExpressionType::Cast: {
      const auto cast_expression = std::dynamic_pointer_cast<CastExpression>(expression);
      stream << "cast_(DataType::" << data_type_to_string.left(cast_expression->data_type);
      stream << expression_to_cpp(cast_expression->argument(), column_expression_names);
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
      stream << column_expression_names.at(lqp_column_expression);
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
        case PredicateCondition::Equals: stream << "equals_"; break;
        case PredicateCondition::NotEquals: stream << "not_equals_"; break;
        case PredicateCondition::LessThan: stream << "less_than_"; break;
        case PredicateCondition::LessThanEquals: stream << "less_than_equals_"; break;
        case PredicateCondition::GreaterThan: stream << "greater_than_"; break;
        case PredicateCondition::GreaterThanEquals: stream << "greater_than_equals_"; break;
        case PredicateCondition::Between: stream << "between_"; break;
        case PredicateCondition::In: stream << "in_"; break;
        case PredicateCondition::NotIn: stream << "not_in_"; break;
        case PredicateCondition::Like: stream << "like_"; break;
        case PredicateCondition::NotLike: stream << "not_like_"; break;
        case PredicateCondition::IsNull: stream << "is_not_null_"; break;
        case PredicateCondition::IsNotNull: stream << "is_not_null_"; break;
      }
    } break;

    case ExpressionType::PQPSelect: {
      //const auto concrete_expression = std::dynamic_pointer_cast<PQPSelectExpression>(expression);
    } break;

    case ExpressionType::LQPSelect: {
      //const auto concrete_expression = std::dynamic_pointer_cast<LQPSelectExpression>(expression);
    } break;

    case ExpressionType::UnaryMinus: {
      stream << "unary_minus_";
    } break;

    case ExpressionType::Value: {
      const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(expression);
      if (value_expression->value.type() == typeid(std::string)) {
        stream << "\"" << value_expression->value << "\"";
      } else {
        stream << value_expression->value;
      }
    } break;
  }

  if (!expression->arguments.empty()) {
    stream << "(";
    for (const auto &argument : expression->arguments) {
      stream << expression_to_cpp(argument, column_expression_names);
      if (argument != expression->arguments.back()) {
        stream << ", ";
      }
    }
    stream << ")";
  }

  return stream.str();
}

std::string lqp_to_cpp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  State state;

  lqp_to_cpp_impl(lqp, "lqp", state);

  return state.stream.str();
}

}  // namespace opossum