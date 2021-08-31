#pragma once

#include "expression/abstract_expression.hpp"

namespace opossum {

struct OrderDependency {
  OrderDependency(ExpressionList init_determinants, ExpressionList init_dependents);

  bool operator==(const OrderDependency& other) const;
  bool operator!=(const OrderDependency& other) const;
  size_t hash() const;

  ExpressionList determinants;
  ExpressionList dependents;
};

std::ostream& operator<<(std::ostream& stream, const OrderDependency& expression);

/**
 * @return The given FDs as an unordered set in an inflated form.
 *         We consider FDs as inflated when they have a single dependent expression only. Therefore, inflating an FD
 *         works as follows:
 *                                                      {a} => {b}
 *                             {a} => {b, c, d}   -->   {a} => {c}
 *                                                      {a} => {d}
 */
std::unordered_set<OrderDependency> inflate_ods(const std::vector<OrderDependency>& ods);

/**
 * @return Reduces the given vector of FDs, so that there are no more FD objects with the same determinant expressions.
 *         As a result, FDs become deflated as follows:
 *
 *                             {a} => {b}
 *                             {a} => {c}         -->   {a} => {b, c, d}
 *                             {a} => {d}
 */
std::vector<OrderDependency> deflate_ods(const std::vector<OrderDependency>& ods);

/**
 * @return Unified FDs from the given @param fds_a and @param fds_b vectors. FDs with the same determinant
 *         expressions are merged into single objects by merging their dependent expressions.
 */
std::vector<OrderDependency> union_ods(const std::vector<OrderDependency>& ods_a,
                                       const std::vector<OrderDependency>& ods_b);

/**
 * @return Returns FDs that are included in both of the given vectors.
 */
std::vector<OrderDependency> intersect_ods(const std::vector<OrderDependency>& ods_a,
                                           const std::vector<OrderDependency>& ods_b);

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
struct hash<opossum::OrderDependency> {
  size_t operator()(const opossum::OrderDependency& od) const;
};

}  // namespace std
