#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

#include "optimizer/chunk_statistics/abstract_filter.hpp"

namespace opossum {

class BaseColumn;

/**
 * Container class that holds a set of filters with statistical information about a
 * certain chunks column. Is part of ChunkStatistics.
 */
class ChunkColumnStatistics final {
 public:
  static std::shared_ptr<ChunkColumnStatistics> build_statistics(DataType data_type,
                                                                 std::shared_ptr<BaseColumn> column);

  void add_filter(std::shared_ptr<AbstractFilter> filter);

  /**
   * calls can_prune on each filter in this object
  */
  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const;

 protected:
  std::vector<std::shared_ptr<AbstractFilter>> _filters;
};
}  // namespace opossum
