#pragma once

#include "cardinality.hpp"
#include "selectivity.hpp"

namespace opossum {

/**
 * TODO(moritz) doc
 * @param selectivity
 * @param value_count
 * @param distinct_count
 * @return
 */
Cardinality scale_distinct_count(Selectivity selectivity, Cardinality value_count, Cardinality distinct_count);

}  // namespace opossum