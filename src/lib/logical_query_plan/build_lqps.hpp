#pragma once

#include <memory>

#include "types.hpp"
#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

class AbstractLQPNode;
class LQPExpression;
class ProjectionNode;
class PredicateNode;
class StoredTableNode;

std::shared_ptr<PredicateNode> make_predicate_node(const LQPColumnReference& column_reference, const PredicateCondition predicate_condition, const AllParameterVariant& value, const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<PredicateNode> make_predicate_node(const LQPColumnReference& column_reference, const PredicateCondition predicate_condition, const AllParameterVariant& value, const std::optional<AllTypeVariant>& value2, const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<ProjectionNode> make_star_projection_node(const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<ProjectionNode> make_projection_node(const std::vector<std::shared_ptr<LQPExpression>>& column_expressions, const std::shared_ptr<AbstractLQPNode>& child);
std::shared_ptr<StoredTableNode> make_stored_table_node(const std::string& table_name, const std::optional<std::string>& alias = std::nullopt);

}  // namespace opossum
