#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

class BaseChunkColumnStatistics : public std::enable_shared_from_this<BaseChunkColumnStatistics> {
 public:
  virtual ~BaseChunkColumnStatistics() = default;

  virtual AllTypeVariant min() const = 0;
  virtual AllTypeVariant max() const = 0;
};

template <typename T>
class ChunkColumnStatistics : public BaseChunkColumnStatistics {
 public:
  ChunkColumnStatistics(const T& min, const T& max) : _min(min), _max(max) {}
  virtual ~ChunkColumnStatistics() = default;

  AllTypeVariant min() const override { return _min; }
  AllTypeVariant max() const override { return _max; }

 protected:
  T _min;
  T _max;
};

class ChunkStatistics : public std::enable_shared_from_this<ChunkStatistics> {
 public:
  ChunkStatistics(std::vector<std::shared_ptr<BaseChunkColumnStatistics>> stats) : _statistics(stats) {}

  const std::vector<std::shared_ptr<BaseChunkColumnStatistics>>& statistics() { return _statistics; }

 protected:
  std::vector<std::shared_ptr<BaseChunkColumnStatistics>> _statistics;
};
}
