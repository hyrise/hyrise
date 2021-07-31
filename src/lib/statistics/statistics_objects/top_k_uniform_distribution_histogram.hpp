#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_histogram.hpp"
#include "types.hpp"
#include "generic_histogram.hpp"

namespace opossum {

const size_t TOP_K_DEFAULT = 100;

class Table;

/**
 *  Top K histogram. 
 *  This histogram saves the 100 most common values as single bins in a GenericHistogram and all non Top-K values as bins in between.
 */
template <typename T>
class TopKUniformDistributionHistogram {
 public:
  /**
   * Create an Top-K histogram as a GenericHistogram for a column (spanning all Segments) of a Table
   */
  static std::shared_ptr<GenericHistogram<T>> from_column(const Table& table, const ColumnID column_id,
                                                                     const HistogramDomain<T>& domain = {});

};

}  // namespace opossum
