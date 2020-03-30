#pragma once

#include <array>
#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "storage/pos_lists/rowid_pos_list.hpp"
#include "types.hpp"

namespace opossum {

// The SegmentAccessCounter is a collection of counters to count how often a segment is accessed.
// It contains several counters (see AccessType) to differentiate between different access types, like
// sequential or random access. The individual counters can be accessed using the [] operator.
// The counters are currently updated by the iterators, segment accessors or from within the segment itself.
class SegmentAccessCounter {
  friend class SegmentAccessCounterTest;

 public:
  using CounterType = std::atomic_uint64_t;

  enum class AccessType {
    Point /* Single point access */,
    Sequential /* 0, 1, 1, 2, 3, 4 */,
    Monotonic /* 0, 0, 1, 2, 4, 8, 17 */,
    Random /* 0, 1, 0, 42 */,
    Dictionary /* Used to count accesses to the dictionary of the dictionary segment */,
    Count /* Dummy entry to describe the number of elements in this enum class. */
  };

  inline static const std::map<AccessType, const char*> access_type_string_mapping = {
      {AccessType::Point, "Point"},
      {AccessType::Sequential, "Sequential"},
      {AccessType::Monotonic, "Monotonic"},
      {AccessType::Random, "Random"},
      {AccessType::Dictionary, "Dictionary"}};

  SegmentAccessCounter();
  SegmentAccessCounter(const SegmentAccessCounter& other);
  SegmentAccessCounter& operator=(const SegmentAccessCounter& other);

  CounterType& operator[](const AccessType type);
  const CounterType& operator[](const AccessType type) const;

  // For a given position list, this determines whether its entries are in sequential, monotonic, or random order.
  // It only looks at the first n values.
  static AccessType access_type(const AbstractPosList& positions);

  std::string to_string() const;

 private:
  std::array<CounterType, static_cast<size_t>(AccessType::Count)> _counters = {};

  // For access pattern analysis: The following enum is used used to determine how an iterator iterates over its
  // elements. This is done by analysing the first elements in a given PosList and a state machine, defined below.
  // There are six AccessPatterns:
  // 0 (point access), an empty sequence or a sequence accessing only a single point
  // 1 (sequentially increasing), difference between two neighboring elements is 0 or 1.
  // 2 (randomly increasing)
  // 3 (sequentially decreasing), difference between two neighboring elements is -1 or 0.
  // 4 (randomly decreasing)
  // 5 (random access)
  enum class AccessPattern {
    Point,
    SequentiallyIncreasing,
    RandomlyIncreasing,
    SequentiallyDecreasing,
    RandomlyDecreasing,
    Random
  };

  static AccessPattern _access_pattern(const AbstractPosList& positions);

  void _set_counters(const SegmentAccessCounter& counter);
};

}  // namespace opossum
