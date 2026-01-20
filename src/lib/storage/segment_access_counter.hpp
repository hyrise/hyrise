#pragma once

#include <array>
#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "magic_enum/magic_enum.hpp"

#include "storage/pos_lists/row_id_pos_list.hpp"
#include "types.hpp"

namespace hyrise {

// The SegmentAccessCounter is a collection of counters to count how often a segment is accessed.
// It contains several counters (see AccessType) to differentiate between different access types, like
// sequential or random access. The individual counters can be accessed using the [] operator.
// The counters are currently updated by the iterators, segment accessors or from within the segment itself.
class SegmentAccessCounter {
  friend class SegmentAccessCounterTest;

 public:
  using CounterType = std::atomic<uint64_t>;

  enum class AccessType : uint8_t {
    Point /* Single point access */,
    Sequential /* 0, 1, 1, 2, 3, 4 */,
    Monotonic /* 0, 0, 1, 2, 4, 8, 17 */,
    Random /* 0, 1, 0, 42 */,
    Dictionary /* Used to count accesses to the dictionary of the dictionary segment */
  };

  inline static const std::map<AccessType, const char*> access_type_string_mapping = {
      {AccessType::Point, "Point"},
      {AccessType::Sequential, "Sequential"},
      {AccessType::Monotonic, "Monotonic"},
      {AccessType::Random, "Random"},
      {AccessType::Dictionary, "Dictionary"}};

  SegmentAccessCounter();
  ~SegmentAccessCounter() = default;
  SegmentAccessCounter(const SegmentAccessCounter& other);
  SegmentAccessCounter& operator=(const SegmentAccessCounter& other);

  // These two need not be deleted, but they are for the same reason that std::atomic has them deleted
  // You can implement them, but you should think about the implications for this class.
  SegmentAccessCounter(SegmentAccessCounter&&) = delete;
  SegmentAccessCounter& operator=(SegmentAccessCounter&&) = delete;

  bool operator==(const SegmentAccessCounter& other) const;
  bool operator!=(const SegmentAccessCounter& other) const;

  CounterType& operator[](const AccessType type);
  const CounterType& operator[](const AccessType type) const;

  // For a given position list, this determines whether its entries are in sequential, monotonic, or random order.
  // It only looks at the first n values.
  static AccessType access_type(const AbstractPosList& positions);

  std::string to_string() const;

 private:
  std::array<CounterType, magic_enum::enum_count<AccessType>()> _counters = {};

  // For access pattern analysis: The following enum is used used to determine how an iterator iterates over its
  // elements. This is done by analysing the first elements in a given PosList and a state machine, defined below.
  // There are six AccessPatterns:
  // 0 (point access), an empty sequence or a sequence accessing only a single point
  // 1 (sequentially increasing), difference between two neighboring elements is 0 or 1.
  // 2 (monotonically increasing)
  // 3 (sequentially decreasing), difference between two neighboring elements is -1 or 0.
  // 4 (monotonically decreasing)
  // 5 (random access)
  enum class AccessPattern : uint8_t {
    Point,
    SequentiallyIncreasing,
    MonotonicallyIncreasing,
    SequentiallyDecreasing,
    MonotonicallyDecreasing,
    Random
  };

  static AccessPattern _access_pattern(const AbstractPosList& positions);

  void _set_counters(const SegmentAccessCounter& counter);
};

}  // namespace hyrise
