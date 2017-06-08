#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace opossum {

using ChunkID = uint32_t;
using ChunkOffset = uint32_t;
struct RowID {
  ChunkID chunk_id;
  ChunkOffset chunk_offset;

  // Joins need to use RowIDs as keys for maps.
  bool operator<(const RowID &rhs) const {
    return std::tie(chunk_id, chunk_offset) < std::tie(rhs.chunk_id, rhs.chunk_offset);
  }
};

// used to represent NULL values
constexpr ChunkOffset INVALID_CHUNK_OFFSET = std::numeric_limits<ChunkOffset>::max();

constexpr RowID NULL_ROW_ID = RowID{0u, INVALID_CHUNK_OFFSET};

using ColumnID = uint16_t;
using ValueID = uint32_t;  // Cannot be larger than ChunkOffset
using WorkerID = uint32_t;
using NodeID = uint32_t;
using TaskID = uint32_t;
using CpuID = uint32_t;

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

constexpr NodeID INVALID_NODE_ID = std::numeric_limits<NodeID>::max();
constexpr TaskID INVALID_TASK_ID = std::numeric_limits<TaskID>::max();
constexpr CpuID INVALID_CPU_ID = std::numeric_limits<CpuID>::max();
constexpr WorkerID INVALID_WORKER_ID = std::numeric_limits<WorkerID>::max();

constexpr NodeID CURRENT_NODE_ID = std::numeric_limits<NodeID>::max() - 1;
constexpr ValueID NULL_VALUE_ID = std::numeric_limits<ValueID>::max();

// The Scheduler currently supports just these 2 priorities, subject to change.
enum class SchedulePriority {
  Normal = 1,  // Schedule task at the end of the queue
  High = 0     // Schedule task at the beginning of the queue
};

}  // namespace opossum
