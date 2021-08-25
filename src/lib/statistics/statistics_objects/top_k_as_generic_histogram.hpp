#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "generic_histogram.hpp"
#include "types.hpp"

namespace opossum {

const size_t TOP_K_DEFAULT = 100;

class Table;

/**
 *  Top K histogram. 
 *  This histogram saves the TOP_K_DEFAULT most common values as single bins in a GenericHistogram
 *  and all Non-Top K values as bins in between using an uniform distribution assumption.
 *  For more detailed documentation regarding histogram creation see the top_k_as_generic_histogram.cpp.
 */
template <typename T>
class TopKAsGenericHistogram {
 public:
  // Create a Top K Generic Histogram for a column (spanning all Segments) of a Table
  static std::shared_ptr<GenericHistogram<T>> from_column(const Table& table, const ColumnID column_id,
                                                          const HistogramDomain<T>& domain = {});
};

}  // namespace opossum
