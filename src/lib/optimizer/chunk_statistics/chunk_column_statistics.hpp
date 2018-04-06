#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

#include "optimizer/chunk_statistics/abstract_filter.hpp"
#include "utils/create_ptr_aliases.hpp"

namespace opossum {

class BaseColumn;

/**
 * Container class that holds a set of filters with statistical information about a
 * certain chunks column. Is part of ChunkStatistics.
 */
class ChunkColumnStatistics final {
 public:
  static ChunkColumnStatisticsSPtr build_statistics(DataType data_type,
                                                                 BaseColumnSPtr column);

  void add_filter(AbstractFilterSPtr filter);

  /**
   * calls can_prune on each filter in this object
  */
  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const;

 protected:
  std::vector<AbstractFilterSPtr> _filters;
};


}  // namespace opossum
