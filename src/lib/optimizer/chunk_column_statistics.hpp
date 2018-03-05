#pragma once

#include <vector>
#include <memory>

#include "all_type_variant.hpp"
#include "types.hpp"

#include "optimizer/abstract_filter.hpp"

namespace opossum {

class BaseColumn;

class ChunkColumnStatistics {
 public:
  static std::shared_ptr<ChunkColumnStatistics> build_statistics(DataType data_type,
                                                                 std::shared_ptr<BaseColumn> column);

  void add_filter(std::shared_ptr<AbstractFilter> filter);

  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const;

 protected:
  std::vector<std::shared_ptr<AbstractFilter>> _filters;
};
}  // namespace opossum
