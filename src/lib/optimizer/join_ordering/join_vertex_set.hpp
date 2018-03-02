#pragma once

#include "boost/dynamic_bitset.hpp"

namespace opossum {

/**
 * A bitset that represents a subsets of the vertices of a JoinGraph.
 *
 * We use a dynamic bitset here, since it has good support for operations commonly performed on join vertex sets, such
 * as UNION, INTERSECTION, etc.
 */
using JoinVertexSet = boost::dynamic_bitset<>;

}  // namespace opossum
