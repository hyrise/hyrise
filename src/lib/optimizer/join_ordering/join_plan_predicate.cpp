#include "join_plan_predicate.hpp"

#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace {

using namespace opossum;  // NOLINT

// Among the vertices, find the index of the one that contains column_reference
size_t get_vertex_idx(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                      const LQPColumnReference& column_reference) {
  for (size_t vertex_idx = 0; vertex_idx < vertices.size(); ++vertex_idx) {
    if (vertices[vertex_idx]->find_output_column_id(column_reference)) return vertex_idx;
  }
  Fail("Couldn't find column " + column_reference.description() + " among vertices");
  return 0;
}
}  // namespace

namespace opossum {

AbstractJoinPlanPredicate::AbstractJoinPlanPredicate(const JoinPlanPredicateType type) : _type(type) {}

JoinPlanPredicateType AbstractJoinPlanPredicate::type() const { return _type; }

JoinPlanLogicalPredicate::JoinPlanLogicalPredicate(
    const std::shared_ptr<const AbstractJoinPlanPredicate>& left_operand,
    JoinPlanPredicateLogicalOperator logical_operator,
    const std::shared_ptr<const AbstractJoinPlanPredicate>& right_operand)
    : AbstractJoinPlanPredicate(JoinPlanPredicateType::LogicalOperator),
      left_operand(left_operand),
      logical_operator(logical_operator),
      right_operand(right_operand) {}

JoinVertexSet JoinPlanLogicalPredicate::get_accessed_vertex_set(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices) const {
  return left_operand->get_accessed_vertex_set(vertices) | right_operand->get_accessed_vertex_set(vertices);
}

void JoinPlanLogicalPredicate::print(std::ostream& stream, const bool enclosing_braces) const {
  if (enclosing_braces) stream << "(";
  left_operand->print(stream, true);
  switch (logical_operator) {
    case JoinPlanPredicateLogicalOperator::And:
      stream << " AND ";
      break;
    case JoinPlanPredicateLogicalOperator::Or:
      stream << " OR ";
      break;
  }

  right_operand->print(stream, true);
  if (enclosing_braces) stream << ")";
}

bool JoinPlanLogicalPredicate::operator==(const JoinPlanLogicalPredicate& rhs) const {
  return left_operand == rhs.left_operand && logical_operator == rhs.logical_operator &&
         right_operand == rhs.right_operand;
}

JoinPlanAtomicPredicate::JoinPlanAtomicPredicate(const LQPColumnReference& left_operand,
                                                 const PredicateCondition predicate_condition,
                                                 const AllParameterVariant& right_operand)
    : AbstractJoinPlanPredicate(JoinPlanPredicateType::Atomic),
      left_operand(left_operand),
      predicate_condition(predicate_condition),
      right_operand(right_operand) {
  DebugAssert(predicate_condition != PredicateCondition::Between,
              "Between not supported in JoinPlanPredicate, should be split up into two predicates");
}

JoinVertexSet JoinPlanAtomicPredicate::get_accessed_vertex_set(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices) const {
  JoinVertexSet vertex_set{vertices.size()};
  vertex_set.set(get_vertex_idx(vertices, left_operand));
  if (is_lqp_column_reference(right_operand)) {
    vertex_set.set(get_vertex_idx(vertices, boost::get<LQPColumnReference>(right_operand)));
  }
  return vertex_set;
}

void JoinPlanAtomicPredicate::print(std::ostream& stream, const bool enclosing_braces) const {
  stream << left_operand.description() << " ";
  stream << predicate_condition_to_string.left.at(predicate_condition) << " ";
  stream << right_operand;
}

bool JoinPlanAtomicPredicate::operator==(const JoinPlanAtomicPredicate& rhs) const {
  return left_operand == rhs.left_operand && predicate_condition == rhs.predicate_condition &&
         right_operand == rhs.right_operand;
}

}  // namespace opossum
