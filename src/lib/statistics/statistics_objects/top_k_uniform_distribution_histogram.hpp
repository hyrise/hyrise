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
 *  This histogram saves the TOP_K_DEFAULT most common values as single bins in a GenericHistogram and all non Top-K values as bins in between.
 *  Histogram is created as follows:
 *  Each Top K value is modeled as one bin with height as its stored count.
 *  One bin is created for all non Top K values between two Top K bins using an uniform distribution assumption. 
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
