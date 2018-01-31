#pragma once

#include <memory>

#include "types.hpp"
#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "lqp_column_reference.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/sort_node.hpp"

namespace opossum {

class AbstractLQPNode;
class CreateViewNode;
class InsertNode;
class LimitNode;
class LQPExpression;
class ProjectionNode;
class PredicateNode;
class StoredTableNode;
class UpdateNode;

std::shared_ptr<PredicateNode> make_predicate_node(const LQPColumnReference& column_reference, const PredicateCondition predicate_condition, const AllParameterVariant& value, const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<PredicateNode> make_predicate_node(const LQPColumnReference& column_reference, const PredicateCondition predicate_condition, const AllParameterVariant& value, const std::optional<AllTypeVariant>& value2, const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<ProjectionNode> make_star_projection_node(const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<ProjectionNode> make_projection_node(const std::vector<std::shared_ptr<LQPExpression>>& column_expressions, const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<StoredTableNode> make_stored_table_node(const std::string& table_name, const std::optional<std::string>& alias = std::nullopt);
std::shared_ptr<SortNode> make_sort_node(const OrderByDefinitions& order_by_definitions, const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<JoinNode> make_join_node(const JoinMode join_mode, const LQPColumnReferencePair& join_column_references,
                                         const PredicateCondition predicate_condition, const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child);
std::shared_ptr<JoinNode> make_join_node(const JoinMode join_mode, const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child);
std::shared_ptr<LimitNode> make_limit_node(const size_t num_rows, const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<InsertNode> make_insert_node(const std::string& table_name, const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<UpdateNode> make_update_node(const std::string& table_name, const std::vector<std::shared_ptr<LQPExpression>>& column_expressions, const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<CreateViewNode> make_create_view_node(const std::string& view_name, std::shared_ptr<const AbstractLQPNode> lqp);

}  // namespace opossum
