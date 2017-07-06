#pragma once

#include <boost/serialization/strong_typedef.hpp>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "strong_typedef.hpp"

namespace opossum {

//
// We use STRONG_TYPEDEF to avoid things like adding chunk ids and value ids.
// Because implicit constructors are deleted, you cannot initialize a ChunkID
// like this
//   ChunkId x = 3;
// but need to use
//   ChunkId x{3};
//
// WorkerID, TaskID, CommitID, and TransactionID are used in std::atomics and
// therefore need to be trivially copyable. That's currently not possible with
// the strong typedef (as far as I know).
// TODO(anyone): Also, strongly typing ChunkOffset causes a lot of errors in
// the group key and adaptive radix tree implementations. Unfortunately, I
// wasn't able to properly resolve these issues because I am not familiar with
// the code there

STRONG_TYPEDEF(uint32_t, ChunkID);
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

STRONG_TYPEDEF(uint16_t, ColumnID);
STRONG_TYPEDEF(uint32_t, ValueID);  // Cannot be larger than ChunkOffset
using WorkerID = uint32_t;
STRONG_TYPEDEF(uint32_t, NodeID);
using TaskID = uint32_t;
STRONG_TYPEDEF(int32_t, CpuID);

// When changing these to 64-bit types, reading and writing to them might not be atomic anymore.
// Among others, the validate operator might break when another operator is simultaneously writing begin or end CIDs.
using CommitID = uint32_t;
using TransactionID = uint32_t;

using StringLength = uint16_t;     // The length of column value strings must fit in this type.
using ColumnNameLength = uint8_t;  // The length of column names must fit in this type.
using AttributeVectorWidth = uint8_t;

using PosList = std::vector<RowID>;

class ColumnName {
 public:
  explicit ColumnName(const std::string &name) : _name(name) {}

  operator std::string() const { return _name; }

 protected:
  const std::string _name;
};

constexpr NodeID INVALID_NODE_ID{std::numeric_limits<NodeID::base_type>::max()};
constexpr TaskID INVALID_TASK_ID{std::numeric_limits<TaskID>::max()};
constexpr CpuID INVALID_CPU_ID{std::numeric_limits<CpuID::base_type>::max()};
constexpr WorkerID INVALID_WORKER_ID{std::numeric_limits<WorkerID>::max()};

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

enum AggregateFunction { Min, Max, Sum, Avg, Count };

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

}  // namespace opossum
