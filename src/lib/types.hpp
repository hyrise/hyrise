#pragma once

#include <tbb/concurrent_vector.h>
#include <boost/circular_buffer.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <boost/operators.hpp>

#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "strong_typedef.hpp"
#include "utils/assert.hpp"

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
STRONG_TYPEDEF(uint32_t, CpuID);

// Used to identify a Parameter within a (Sub)Select. This can be either a parameter of a Prepared SELECT statement
// `SELECT * FROM t WHERE a > ?` or a correlated parameter in a Subselect.
STRONG_TYPEDEF(size_t, ParameterID);

namespace opossum {

/** We use vectors with custom allocators, e.g, to bind the data object to
 * specific NUMA nodes. This is mainly used in the data objects, i.e.,
 * Chunk, ValueSegment, DictionarySegment, ReferenceSegment and attribute vectors.
 * The PolymorphicAllocator provides an abstraction over several allocation
 * methods by adapting to subclasses of boost::container::pmr::memory_resource.
 */

template <typename T>
using PolymorphicAllocator = boost::container::pmr::polymorphic_allocator<T>;

template <typename T>
using pmr_vector = std::vector<T, PolymorphicAllocator<T>>;

// We are not using PMR here because of the problems described in #281.
// Short version: The current TBB breaks with it, because it needs rebind.
// Once that works, replace the class below with
// using pmr_concurrent_vector = tbb::concurrent_vector<T, PolymorphicAllocator<T>>;
template <typename T>
class pmr_concurrent_vector : public tbb::concurrent_vector<T> {
 public:
  pmr_concurrent_vector(PolymorphicAllocator<T> alloc = {}) : pmr_concurrent_vector(0, alloc) {}  // NOLINT
  pmr_concurrent_vector(std::initializer_list<T> init_list, PolymorphicAllocator<T> alloc = {})
      : tbb::concurrent_vector<T>(init_list), _alloc(alloc) {}         // NOLINT
  pmr_concurrent_vector(size_t n, PolymorphicAllocator<T> alloc = {})  // NOLINT
      : pmr_concurrent_vector(n, T{}, alloc) {}
  pmr_concurrent_vector(size_t n, T val, PolymorphicAllocator<T> alloc = {})  // NOLINT
      : tbb::concurrent_vector<T>(n, val), _alloc(alloc) {}
  pmr_concurrent_vector(tbb::concurrent_vector<T> other, PolymorphicAllocator<T> alloc = {})  // NOLINT
      : tbb::concurrent_vector<T>(other), _alloc(alloc) {}
  pmr_concurrent_vector(const std::vector<T>& values, PolymorphicAllocator<T> alloc = {})  // NOLINT
      : tbb::concurrent_vector<T>(values.begin(), values.end()), _alloc(alloc) {}
  pmr_concurrent_vector(std::vector<T>&& values, PolymorphicAllocator<T> alloc = {})  // NOLINT
      : tbb::concurrent_vector<T>(std::make_move_iterator(values.begin()), std::make_move_iterator(values.end())),
        _alloc(alloc) {}

  template <class I>
  pmr_concurrent_vector(I first, I last, PolymorphicAllocator<T> alloc = {})
      : tbb::concurrent_vector<T>(first, last), _alloc(alloc) {}

  const PolymorphicAllocator<T>& get_allocator() const { return _alloc; }

 protected:
  PolymorphicAllocator<T> _alloc;
};

template <typename T>
using pmr_ring_buffer = boost::circular_buffer<T, PolymorphicAllocator<T>>;

using ChunkOffset = uint32_t;

constexpr ChunkOffset INVALID_CHUNK_OFFSET{std::numeric_limits<ChunkOffset>::max()};
constexpr ChunkID INVALID_CHUNK_ID{std::numeric_limits<ChunkID::base_type>::max()};

struct RowID {
  ChunkID chunk_id{INVALID_CHUNK_ID};
  ChunkOffset chunk_offset{INVALID_CHUNK_OFFSET};

  RowID() = default;

  RowID(const ChunkID chunk_id, const ChunkOffset chunk_offset) : chunk_id(chunk_id), chunk_offset(chunk_offset) {
    DebugAssert((chunk_offset == INVALID_CHUNK_OFFSET) == (chunk_id == INVALID_CHUNK_ID),
                "If you pass in one of the arguments as INVALID/NULL, the other has to be INVALID/NULL as well. This "
                "makes sure there is just one value representing an invalid row id.");
  }

  // Faster than row_id == ROW_ID_NULL, since we only compare the ChunkOffset
  bool is_null() const { return chunk_offset == INVALID_CHUNK_OFFSET; }

  // Joins need to use RowIDs as keys for maps.
  bool operator<(const RowID& other) const {
    return std::tie(chunk_id, chunk_offset) < std::tie(other.chunk_id, other.chunk_offset);
  }

  // Useful when comparing a row ID to NULL_ROW_ID
  bool operator==(const RowID& other) const {
    return std::tie(chunk_id, chunk_offset) == std::tie(other.chunk_id, other.chunk_offset);
  }

  friend std::ostream& operator<<(std::ostream& o, const RowID& row_id) {
    o << "RowID(" << row_id.chunk_id << "," << row_id.chunk_offset << ")";
    return o;
  }
};

using WorkerID = uint32_t;
using TaskID = uint32_t;

// When changing these to 64-bit types, reading and writing to them might not be atomic anymore.
// Among others, the validate operator might break when another operator is simultaneously writing begin or end CIDs.
using CommitID = uint32_t;
using TransactionID = uint32_t;

using AttributeVectorWidth = uint8_t;

using ColumnIDPair = std::pair<ColumnID, ColumnID>;

constexpr NodeID INVALID_NODE_ID{std::numeric_limits<NodeID::base_type>::max()};
constexpr TaskID INVALID_TASK_ID{std::numeric_limits<TaskID>::max()};
constexpr CpuID INVALID_CPU_ID{std::numeric_limits<CpuID::base_type>::max()};
constexpr WorkerID INVALID_WORKER_ID{std::numeric_limits<WorkerID>::max()};
constexpr ColumnID INVALID_COLUMN_ID{std::numeric_limits<ColumnID::base_type>::max()};

constexpr NodeID CURRENT_NODE_ID{std::numeric_limits<NodeID::base_type>::max() - 1};

// ... in ReferenceSegments
const RowID NULL_ROW_ID = RowID{INVALID_CHUNK_ID, INVALID_CHUNK_OFFSET};  // TODO(anyone): Couldn’t use constexpr here

// ... in DictionarySegments
constexpr ValueID NULL_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// The Scheduler currently supports just these 3 priorities, subject to change.
enum class SchedulePriority {
  Default = 1,  // Schedule task at the end of the queue
  High = 0      // Schedule task at the beginning of the queue
};

enum class PredicateCondition {
  Equals,
  NotEquals,
  LessThan,
  LessThanEquals,
  GreaterThan,
  GreaterThanEquals,
  Between,
  In,
  NotIn,
  Like,
  NotLike,
  IsNull,
  IsNotNull
};

bool is_binary_predicate_condition(const PredicateCondition predicate_condition);

// ">" becomes "<" etc.
PredicateCondition flip_predicate_condition(const PredicateCondition predicate_condition);

// ">" becomes "<=" etc.
PredicateCondition inverse_predicate_condition(const PredicateCondition predicate_condition);

enum class JoinMode { Inner, Left, Right, Outer, Cross, Semi, Anti };

enum class UnionMode { Positions };

enum class OrderByMode { Ascending, Descending, AscendingNullsLast, DescendingNullsLast };

enum class TableType { References, Data };

enum class HistogramType { EqualWidth, EqualHeight, EqualDistinctCount };

enum class DescriptionMode { SingleLine, MultiLine };

enum class UseMvcc : bool { Yes = true, No = false };

enum class CleanupTemporaries : bool { Yes = true, No = false };

// Used as a template parameter that is passed whenever we conditionally erase the type of a template. This is done to
// reduce the compile time at the cost of the runtime performance. Examples are iterators, which are replaced by
// AnySegmentIterators that use virtual method calls.
enum class EraseTypes { OnlyInDebug, Always };

class Noncopyable {
 protected:
  Noncopyable() = default;
  Noncopyable(Noncopyable&&) noexcept = default;
  Noncopyable& operator=(Noncopyable&&) noexcept = default;
  ~Noncopyable() = default;
  Noncopyable(const Noncopyable&) = delete;
  const Noncopyable& operator=(const Noncopyable&) = delete;
};

// Dummy type, can be used to overload functions with a variant accepting a Null value
struct Null {};

}  // namespace opossum
