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
 * 
 */
template <typename T>
class TopKUniformDistributionHistogram {
 public:
  /**
   * Create an GenericHistogram for a column (spanning all Segments) of a Table
   * @param max_bin_count   Desired number of bins. Less might be created, but never more. Must not be zero.
   */
  static std::shared_ptr<GenericHistogram<T>> from_column(const Table& table, const ColumnID column_id,
                                                                     const BinID max_bin_count,
                                                                     const HistogramDomain<T>& domain = {});

};

}  // namespace opossum
