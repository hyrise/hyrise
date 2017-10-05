#pragma once

#include <tbb/concurrent_vector.h>

#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <tuple>
#include <vector>

#include "polymorphic_allocator.hpp"
#include "strong_typedef.hpp"

/**
 * We use STRONG_TYPEDEF to avoid things like adding chunk ids and value ids.
 * Because implicit constructors are deleted, you cannot initialize a ChunkID
 * like this
 *   ChunkID x = 3;
 * but need to use
 *   ChunkID x{3}; or
 *   auto x = ChunkID{3};
 *
 * WorkerID, TaskID, CommitID, and TransactionID are used in std::atomics and
 * therefore need to be trivially copyable. That's currently not possible with
 * the strong typedef (as far as I know).
 *
 * TODO(anyone): Also, strongly typing ChunkOffset causes a lot of errors in
 * the group key and adaptive radix tree implementations. Unfortunately, I
 * wasn’t able to properly resolve these issues because I am not familiar with
 * the code there
 */

STRONG_TYPEDEF(uint32_t, ChunkID);
STRONG_TYPEDEF(uint16_t, ColumnID);
STRONG_TYPEDEF(uint32_t, ValueID);  // Cannot be larger than ChunkOffset
STRONG_TYPEDEF(uint32_t, NodeID);
STRONG_TYPEDEF(int32_t, CpuID);

namespace opossum {

/** We use vectors with custom allocators, e.g, to bind the data object to
 * specific NUMA nodes. This is mainly used in the data objects, i.e.,
 * Chunk, ValueColumn, DictionaryColumn, ReferenceColumn and attribute vectors.
 * The PolymorphicAllocator provides an abstraction over several allocation
 * methods by adapting to subclasses of boost::container::pmr::memory_resource.
 */

template <typename T>
using pmr_vector = std::vector<T, PolymorphicAllocator<T>>;

template <typename T>
using pmr_concurrent_vector = tbb::concurrent_vector<T, PolymorphicAllocator<T>>;

using ChunkOffset = uint32_t;

struct RowID {
  ChunkID chunk_id;
  ChunkOffset chunk_offset;

  // Joins need to use RowIDs as keys for maps.
  bool operator<(const RowID &rhs) const {
    return std::tie(chunk_id, chunk_offset) < std::tie(rhs.chunk_id, rhs.chunk_offset);
  }

  // Useful when comparing a row ID to NULL_ROW_ID
  bool operator==(const RowID &rhs) const {
    return std::tie(chunk_id, chunk_offset) == std::tie(rhs.chunk_id, rhs.chunk_offset);
  }
};

using WorkerID = uint32_t;
using TaskID = uint32_t;

// When changing these to 64-bit types, reading and writing to them might not be atomic anymore.
// Among others, the validate operator might break when another operator is simultaneously writing begin or end CIDs.
using CommitID = uint32_t;
using TransactionID = uint32_t;

using StringLength = uint16_t;     // The length of column value strings must fit in this type.
using ColumnNameLength = uint8_t;  // The length of column names must fit in this type.
using AttributeVectorWidth = uint8_t;

using PosList = pmr_vector<RowID>;

constexpr NodeID INVALID_NODE_ID{std::numeric_limits<NodeID::base_type>::max()};
constexpr TaskID INVALID_TASK_ID{std::numeric_limits<TaskID>::max()};
constexpr CpuID INVALID_CPU_ID{std::numeric_limits<CpuID::base_type>::max()};
constexpr WorkerID INVALID_WORKER_ID{std::numeric_limits<WorkerID>::max()};
constexpr ColumnID INVALID_COLUMN_ID{std::numeric_limits<ColumnID::base_type>::max()};

constexpr NodeID CURRENT_NODE_ID{std::numeric_limits<NodeID::base_type>::max() - 1};

// Used to represent NULL values
constexpr ChunkOffset INVALID_CHUNK_OFFSET{std::numeric_limits<ChunkOffset>::max()};

// ... in ReferenceColumns
const RowID NULL_ROW_ID = RowID{ChunkID{0u}, INVALID_CHUNK_OFFSET};  // TODO(anyone): Couldn’t use constexpr here

// ... in DictionaryColumns
constexpr ValueID NULL_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// The Scheduler currently supports just these 2 priorities, subject to change.
enum class SchedulePriority {
  Normal = 1,  // Schedule task at the end of the queue
  High = 0     // Schedule task at the beginning of the queue
};

// Part of AllParameterVariant to reference parameters that will be replaced later.
// When stored in an operator, the operator's recreate method can contain functionality
// that will replace a ValuePlaceholder with an explicit value from a given list of arguments
class ValuePlaceholder {
 public:
  explicit ValuePlaceholder(uint16_t index) : _index(index) {}

  uint16_t index() const { return _index; }

  friend std::ostream &operator<<(std::ostream &o, const ValuePlaceholder &placeholder) {
    o << "?" << placeholder.index();
    return o;
  }

  bool operator==(const ValuePlaceholder &rhs) const { return _index == rhs._index; }

 private:
  uint16_t _index;
};

// TODO(anyone): integrate and replace with ExpressionType
enum class ScanType {
  OpEquals,
  OpNotEquals,
  OpLessThan,
  OpLessThanEquals,
  OpGreaterThan,
  OpGreaterThanEquals,
  OpBetween,
  OpLike
};

enum class ExpressionType {
  /*Any literal value*/
  Literal,
  /*A star as in SELECT * FROM ...*/
  Star,
  /*A parameter used in PreparedStatements*/
  Placeholder,
  /*An identifier for a column*/
  Column,
  /*An identifier for a function, such as COUNT, MIN, MAX*/
  Function,

  /*A subselect*/
  Select,

  /*Arithmetic operators*/
  Addition,
  Subtraction,
  Multiplication,
  Division,
  Modulo,
  Power,

  /*Logical operators*/
  Equals,
  NotEquals,
  LessThan,
  LessThanEquals,
  GreaterThan,
  GreaterThanEquals,
  Like,
  NotLike,
  And,
  Or,
  Between,
  Not,

  /*Set operators*/
  In,
  Exists,

  /*Others*/
  IsNull,
  Case,
  Hint
};

enum class JoinMode { Inner, Left, Right, Outer, Cross, Natural, Self };

enum class AggregateFunction { Min, Max, Sum, Avg, Count };

enum class OrderByMode { Ascending, Descending, AscendingNullsLast, DescendingNullsLast };

class Noncopyable {
 protected:
  Noncopyable() = default;
  Noncopyable(Noncopyable &&) = default;
  Noncopyable &operator=(Noncopyable &&) = default;
  ~Noncopyable() = default;
  Noncopyable(const Noncopyable &) = delete;
  const Noncopyable &operator=(const Noncopyable &) = delete;
};

}  // namespace opossum
