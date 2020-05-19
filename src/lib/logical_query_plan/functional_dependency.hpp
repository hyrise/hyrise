#pragma once

#include "expression/abstract_expression.hpp"

namespace opossum {

/**
 * Models a functional dependency (FD), which consists out of two sets of column expression.
 * The left column set (determinants) unambigiously identifies the right column set (dependents):
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
 *
 * Currently, column expressions are required to be non-nullable to be involved in FDs. As there are
 * strategies to combine both, null values and FDs (e.g. https://arxiv.org/abs/1404.4963), this might
 * change in the future.
 */
struct FunctionalDependency {
  FunctionalDependency(ExpressionUnorderedSet init_determinants, ExpressionUnorderedSet init_dependents);

  bool operator==(const FunctionalDependency& other) const;

  ExpressionUnorderedSet determinants;
  ExpressionUnorderedSet dependents;
};

std::ostream& operator<<(std::ostream& stream, const FunctionalDependency& expression);

}  // namespace opossum
