#pragma once

#include "types.hpp"

namespace opossum {

/**
 * @brief the base class of all table scan impls
 */
class AbstractTableScanImpl {
 public:
  virtual ~AbstractTableScanImpl() = default;

  virtual std::string description() const = 0;

  virtual std::shared_ptr<PosList> scan_chunk(ChunkID chunk_id) = 0;
};

}  // namespace opossum
