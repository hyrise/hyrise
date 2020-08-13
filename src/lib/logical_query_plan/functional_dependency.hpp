#pragma once

#include "expression/abstract_expression.hpp"

namespace opossum {

/**
 * Models a functional dependency (FD), which consists out of two sets of expressions.
 * The left set of expressions (determinants) unambigiously identifies the right set (dependents):
 * {Left} => {Right}
 *
 * Example A:
 * Think of a table with three columns: (Semester, CourseID, Lecturer)
 * The primary key is defined across the first two columns, which leads to the following FD:
 * {Semester, CourseID} => {Lecturer}
 *
 * Example B:
 * Think of a table with four columns: (ISBN, Genre, Author, Author-Nationality)
 * The primary key {ISBN} identifies all other columns. Therefore, we get the following FDs:
 * {ISBN} => {Author}
 * {ISBN} => {Genre}
 * {ISBN} => {Author}
 * {ISBN} => {Author-Nationality}
 * Furthermore, knowing an author, we also know his nationality. Hence, we can specify another FD that applies:
 * {Author} => {Author-Nationality}
 *
 * Currently, the determinant expressions are required to be non-nullable to be involved in FDs.
 * Combining null values and FDs is not trivial. For more reference, see https://arxiv.org/abs/1404.4963.
 */
struct FunctionalDependency {
  FunctionalDependency(ExpressionUnorderedSet init_determinants, ExpressionUnorderedSet init_dependents);

  bool operator==(const FunctionalDependency& other) const;
  bool operator!=(const FunctionalDependency& other) const;

  ExpressionUnorderedSet determinants;
  ExpressionUnorderedSet dependents;
};

std::ostream& operator<<(std::ostream& stream, const FunctionalDependency& expression);

// TODO Doc
std::vector<FunctionalDependency> merge_fds(const std::vector<FunctionalDependency>& fds_a,
                                            const std::vector<FunctionalDependency>& fds_b);

}  // namespace opossum

namespace std {

/**
 * TODO we hash by determinants only
 */
template <>
struct hash<opossum::FunctionalDependency> {
  size_t operator()(const opossum::FunctionalDependency& fd) const;
};

}  // namespace std
