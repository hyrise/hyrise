#pragma once

#include "expression/abstract_expression.hpp"
#include "storage/table_column_id.hpp"

namespace opossum {

struct InclusionDependency {
  InclusionDependency(std::vector<TableColumnID> init_determinants, ExpressionList init_dependents);

  bool operator==(const InclusionDependency& other) const;
  bool operator!=(const InclusionDependency& other) const;
  size_t hash() const;

  std::vector<TableColumnID> determinants;
  ExpressionList dependents;
};

std::ostream& operator<<(std::ostream& stream, const InclusionDependency& expression);

/**
 * @return The given FDs as an unordered set in an inflated form.
 *         We consider FDs as inflated when they have a single dependent expression only. Therefore, inflating an FD
 *         works as follows:
 *                                                      {a} => {b}
 *                             {a} => {b, c, d}   -->   {a} => {c}
 *                                                      {a} => {d}
 */
std::unordered_set<InclusionDependency> inflate_inds(const std::vector<InclusionDependency>& inds);

/**
 * @return Reduces the given vector of FDs, so that there are no more FD objects with the same determinant expressions.
 *         As a result, FDs become deflated as follows:
 *
 *                             {a} => {b}
 *                             {a} => {c}         -->   {a} => {b, c, d}
 *                             {a} => {d}
 */
std::vector<InclusionDependency> deflate_inds(const std::vector<InclusionDependency>& inds);

/**
 * @return Unified FDs from the given @param fds_a and @param fds_b vectors. FDs with the same determinant
 *         expressions are merged into single objects by merging their dependent expressions.
 */
std::vector<InclusionDependency> union_inds(const std::vector<InclusionDependency>& inds_a,
                                            const std::vector<InclusionDependency>& inds_b);

/**
 * @return Returns FDs that are included in both of the given vectors.
 */
std::vector<InclusionDependency> intersect_inds(const std::vector<InclusionDependency>& inds_a,
                                                const std::vector<InclusionDependency>& inds_b);

/**
 * Future Work: Transitive FDs
 * Given two or more FDs, it might become possible to derive transitive FDs from them.
 * For example: {a} => {b} and
 *              {b} => {c} lead to the following transitive FD: {a} => {c}
 * To check for transitive FDs, we could provide a function called `fds_apply(fds, dependent, dependee)` that takes a
 * set of FDs and two expressions to see if dependee is dependent on dependent.
 */

}  // namespace opossum

namespace std {

/**
 * Please note: FDs with the same determinant expressions are expected to be merged into single FD objects (e.g. for
 * unordered sets). Therefore, we hash the determinant expressions only.
 */
template <>
struct hash<opossum::InclusionDependency> {
  size_t operator()(const opossum::InclusionDependency& ind) const;
};

}  // namespace std
