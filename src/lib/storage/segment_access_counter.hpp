#pragma once

#include <array>
#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "storage/pos_list.hpp"
#include "types.hpp"

namespace opossum {
class Table;

// The SegmentAccessCounter is a collection of counters to register how often and how a segment is accessed.
class SegmentAccessCounter {
  friend class SegmentAccessCounterTest;

 public:
  using CounterType = std::atomic_uint64_t;

  enum class AccessType {
    Point /* Single point access */,
    Sequential /* 0, 1, 1, 2, 3, 4 */,
    Increasing /* 0, 0, 1, 2, 4, 8, 17 */ /* or decreasing */,
    Random /* 0, 1, 0, 42 */,
    Dictionary,
    Count /* Dummy entry to describe the number of elements in this enum class. */
  };

  inline static const std::array<std::string, (size_t)AccessType::Count> access_type_string_mapping = {
    "Point", "Sequential", "SequentiallyIncreasing", "Random"
  };

  SegmentAccessCounter();
  SegmentAccessCounter(const SegmentAccessCounter& counter);
  SegmentAccessCounter& operator=(const SegmentAccessCounter& counter);

  CounterType& get(const AccessType type);
  const CounterType& get(const AccessType type) const;

  static AccessType access_type(const PosList& positions);

  uint64_t sum() const;

  std::string to_string() const;

  void reset();

  // resets the counters for the given tables
  static void reset(const std::map<std::string, std::shared_ptr<Table>>& tables);

 private:
  std::array<CounterType, (size_t)AccessType::Count> _counters;

  // For access pattern analysis: The following enums are used to determine how an iterator iterates over its elements.
  // This is done by analysing the first elements in a given PosList and a state machine, defined below.
  // There are six AccessPatterns:
  // 0 (point access), an empty sequence or a sequence accessing only a single point
  // 1 (sequentially increasing), difference between two neighboring elements is 0 or 1.
  // 2 (randomly increasing)
  // 3 (sequentially decreasing), difference between two neighboring elements is -1 or 0.
  // 4 (randomly decreasing)
  // 5 (random access)
  enum class AccessPattern { Point, SequentiallyIncreasing, RandomlyIncreasing, SequentiallyDecreasing,
    RandomlyDecreasing, Random };

  static AccessPattern _access_pattern(const PosList& positions);

  void _set_counters(const SegmentAccessCounter& counter);
};

}  // namespace opossum
