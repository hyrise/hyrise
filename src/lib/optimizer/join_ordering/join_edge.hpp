#pragma once

#include <iostream>
#include <ostream>

#include "boost/dynamic_bitset.hpp"

namespace opossum {

class AbstractJoinPlanPredicate;

/**
 * Represents a (hyper)edge between the vertices in vertex_set and a set of predicates. These predicates must all
 * operate precisely on all of the vertices in vertex_set.
 */
 public:
  explicit JoinEdge(const boost::dynamic_bitset<>& vertex_set);

  void print(std::ostream& stream = std::cout) const;

  const boost::dynamic_bitset<> vertex_set;
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;
};

}  // namespace opossum
