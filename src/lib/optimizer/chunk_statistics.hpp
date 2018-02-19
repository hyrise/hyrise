#pragma once

#include <exception>
#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

class BaseFilter : public std::enable_shared_from_this<BaseFilter> {
 public:
  virtual ~BaseFilter() = default;

  virtual bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const = 0;
};

class ChunkColumnStatistics {
 public:
  void add_filter(std::shared_ptr<BaseFilter> filter) {
    _filters.emplace_back(filter);
  }

  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const {
    for(const auto& filter : _filters) {
      if(filter->can_prune(value, predicate_type)) {
        return true;
      }
    }
    return false;
  }

  protected:
  std::vector<std::shared_ptr<BaseFilter>> _filters;
};

template <typename T>
class MinMaxFilter : public BaseFilter {
 public:

  MinMaxFilter(T min, T max) : _min(min), _max(max) {};
  virtual ~MinMaxFilter() = default;

  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const override {
    T t_value = type_cast<T>(value);
    // Operators work as follows: value_from_table <operator> t_value
    // e.g. OpGreaterThan: value_from_table > t_value
    // thus we can exclude chunk if t_value >= _max since then no value from the table can be greater than t_value
    switch (predicate_type) {
      case PredicateCondition::GreaterThan:
        return t_value >= _max;
      case PredicateCondition::GreaterThanEquals:
        return t_value > _max;
      case PredicateCondition::LessThan:
        return t_value <= _min;
      case PredicateCondition::LessThanEquals:
        return t_value < _min;
      case PredicateCondition::Equals:
        return t_value < _min || t_value > _max;
      default:
        return false;
    }
  }
 protected:
  T _min;
  T _max;
};

template <typename T>
class RangeFilter : public BaseFilter {
 public:
  RangeFilter(std::vector<std::tuple<T,T>> ranges) : _ranges(ranges) {};
  virtual ~RangeFilter() = default;
  
  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const override {
    T t_value = type_cast<T>(value);
    // Operators work as follows: value_from_table <operator> t_value
    // e.g. OpGreaterThan: value_from_table > t_value
    // thus we can exclude chunk if t_value >= _max since then no value from the table can be greater than t_value
    bool prunable = false;
    for(const auto& bounds : _ranges) {
      auto& [min,max] = bounds;
      switch (predicate_type) {
        case PredicateCondition::Equals:
          prunable |= t_value < min || t_value > max;
          break;
      }
    }
    return prunable;
  }

 protected:
  std::vector<std::tuple<T,T>> _ranges; 
};

class ChunkStatistics : public std::enable_shared_from_this<ChunkStatistics> {
 public:
  explicit ChunkStatistics(std::vector<std::shared_ptr<ChunkColumnStatistics>> stats) : _statistics(stats) {}

  const std::vector<std::shared_ptr<ChunkColumnStatistics>>& statistics() const { return _statistics; }

  bool can_prune(const ColumnID column_id, const AllTypeVariant& value, const PredicateCondition predicate_type) const;

  std::string to_string() const;

 protected:
  std::vector<std::shared_ptr<ChunkColumnStatistics>> _statistics;
};
}  // namespace opossum
