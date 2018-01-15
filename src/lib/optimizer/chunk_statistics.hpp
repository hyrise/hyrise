#pragma once

#include <memory>
#include <vector>
#include <exception>

#include "all_type_variant.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

class BaseChunkColumnStatistics : public std::enable_shared_from_this<BaseChunkColumnStatistics> {
 public:
  virtual ~BaseChunkColumnStatistics() = default;

  virtual AllTypeVariant min() const = 0;
  virtual AllTypeVariant max() const = 0;

  virtual bool can_prune(const AllTypeVariant& value, const ScanType scan_type) const = 0;
};

template <typename T>
class ChunkColumnStatistics : public BaseChunkColumnStatistics {
 public:
  ChunkColumnStatistics(const T& min, const T& max) : _min(min), _max(max) {}
  virtual ~ChunkColumnStatistics() = default;

  AllTypeVariant min() const override { return _min; }
  AllTypeVariant max() const override { return _max; }

  bool can_prune(const AllTypeVariant& value, const ScanType scan_type) const override {
    T t_value = type_cast<T>(value);
    switch (scan_type) {
      case ScanType::OpGreaterThan:
        return t_value > _max;
      default: throw std::logic_error("not implemented");
    }
  }

 protected:
  T _min;
  T _max;
};

class ChunkStatistics : public std::enable_shared_from_this<ChunkStatistics> {
 public:
  ChunkStatistics(std::vector<std::shared_ptr<BaseChunkColumnStatistics>> stats) : _statistics(stats) {}

  const std::vector<std::shared_ptr<BaseChunkColumnStatistics>>& statistics() const { return _statistics; }

  bool can_prune(const ScanType scan_type, const ColumnID column_id, const AllTypeVariant& value) const;

 protected:
  std::vector<std::shared_ptr<BaseChunkColumnStatistics>> _statistics;
};
}
