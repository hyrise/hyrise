#pragma once

#include "expression/abstract_expression.hpp"

namespace hyrise {

/**
 * Models a functional dependency (FD), which consists out of two sets of expressions. The left set of expressions
 * (determinants) unambigiously identifies the right set (dependents):
 *   {Left} => {Right}
 *
 * Example A:
 * Think of a table with three columns: CourseLecturerAssignment(Semester, CourseID, Lecturer). The primary key is
 * defined across the first two columns, which leads to the following FD:
 *   {Semester, CourseID} => {Lecturer}
 *
 * Example B:
 * Think of a table with four columns: Book(ISBN, Genre, Author, Author-Nationality). The primary key {ISBN} identifies
 * all other columns. Therefore, we get the following FDs:
 *   {ISBN} => {Author}
 *   {ISBN} => {Genre}
 *   {ISBN} => {Author}
 *   {ISBN} => {Author-Nationality}
 * Furthermore, knowing an author, we also know his nationality. Hence, we can specify another FD that applies:
 *   {Author} => {Author-Nationality}
 *
 * Currently, the determinant expressions are required to be non-nullable to be involved in FDs. Combining null values
 * and FDs is not trivial. For more reference, see https://arxiv.org/abs/1404.4963.
 *
 * If the FD may become invalid in the future (because it is not based on a schema constraint, but on the data
 * incidentally fulfilling the constraint at the moment), the FD is marked as being not permanent.
 * This information is important because query plans that were optimized using a non-permanent FD are probably
 * not cacheable.
 */
struct FunctionalDependency {
  FunctionalDependency(ExpressionUnorderedSet init_determinants, ExpressionUnorderedSet init_dependents);
  FunctionalDependency(ExpressionUnorderedSet init_determinants, ExpressionUnorderedSet init_dependents,
                       bool permanent);

  bool operator==(const FunctionalDependency& other) const;
  bool operator!=(const FunctionalDependency& other) const;
  size_t hash() const;

  bool is_permanent() const;

  ExpressionUnorderedSet determinants;
  ExpressionUnorderedSet dependents;

  bool permanent;
};

std::ostream& operator<<(std::ostream& stream, const FunctionalDependency& expression);

using FunctionalDependencies = std::unordered_set<FunctionalDependency>;

/**
 * @return The given FDs as an unordered set in an inflated form.
 *         We consider FDs as inflated when they have a single dependent expression only. Therefore, inflating an FD
 *         works as follows:
 *                                                      {a} => {b}
 *                             {a} => {b, c, d}   -->   {a} => {c}
 *                                                      {a} => {d}
 */
FunctionalDependencies inflate_fds(const FunctionalDependencies& fds);

/**
 * @return Reduces the given vector of FDs, so that there are no more FD objects with the same determinant expressions.
 *         As a result, FDs become deflated as follows:
 *
 *                             {a} => {b}
 *                             {a} => {c}         -->   {a} => {b, c, d}
 *                             {a} => {d}
 */
FunctionalDependencies deflate_fds(const FunctionalDependencies& fds);

/**
 * @return Unified FDs from the given @param fds_a and @param fds_b vectors. FDs with the same determinant
 *         expressions are merged into single objects by merging their dependent expressions.
 */
FunctionalDependencies union_fds(const FunctionalDependencies& fds_a, const FunctionalDependencies& fds_b);

/**
 * @return Returns FDs that are included in both of the given vectors.
 */
FunctionalDependencies intersect_fds(const FunctionalDependencies& fds_a, const FunctionalDependencies& fds_b);

/**
 * Future Work: Transitive FDs
 * Given two or more FDs, it might become possible to derive transitive FDs from them.
 * For example: {a} => {b} and
 *              {b} => {c} lead to the following transitive FD: {a} => {c}
 * To check for transitive FDs, we could provide a function called `fds_apply(fds, dependent, dependee)` that takes a
 * set of FDs and two expressions to see if dependee is dependent on dependent.
 */

}  // namespace hyrise

namespace std {

/**
 * Please note: FDs with the same determinant expressions are expected to be merged into single FD objects (e.g. for
 * unordered sets). Therefore, we hash the determinant expressions only.
 */
template <>
struct hash<hyrise::FunctionalDependency> {
  size_t operator()(const hyrise::FunctionalDependency& fd) const;
};

}  // namespace std
