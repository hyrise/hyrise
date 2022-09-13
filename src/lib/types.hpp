#pragma once

#include <tbb/concurrent_vector.h>

#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <boost/bimap.hpp>
#include <boost/circular_buffer.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <boost/operators.hpp>
#include <boost/version.hpp>

#include "strong_typedef.hpp"
#include "utils/assert.hpp"

/**
 * We use STRONG_TYPEDEF to avoid things like adding chunk ids and value ids. Because implicit constructors are
 * deleted, you cannot initialize a ChunkID like this:
 *   ChunkID x = 3;
 * but need to use
 *   auto x = ChunkID{3};
 * In some cases (e.g., when narrowing data types), casting to the base_type first might be necessary, e.g.:
 *   ChunkID{static_cast<ChunkID::base_type>(size_t{17})}
 *
 * We prefer strong typedefs whenever they are applicable and make sense. However, there are cases where we cannot
 * directly use them. For example, in std::atomics when we want to use C++'s specializations for integral types (e.g.,
 * `++task_id` with `std::atomic<TaskID> task_id`). Therefore, we use the base_type in atomics (e.g.,
 * `std::atomic<TaskID::base_type> task_id`).
 */

STRONG_TYPEDEF(uint32_t, ChunkID);
STRONG_TYPEDEF(uint16_t, ColumnID);
STRONG_TYPEDEF(hyrise::ColumnID::base_type, ColumnCount);
STRONG_TYPEDEF(uint32_t, ValueID);  // Cannot be larger than ChunkOffset
STRONG_TYPEDEF(uint32_t, NodeID);
STRONG_TYPEDEF(uint32_t, CpuID);
STRONG_TYPEDEF(uint32_t, WorkerID);
STRONG_TYPEDEF(uint32_t, TaskID);
STRONG_TYPEDEF(uint32_t, ChunkOffset);

// When changing the following two strong typedefs to 64-bit types, please be aware that both are used with
// std::atomics and not all platforms that Hyrise runs on support atomic 64-bit instructions. Any Intel and AMD CPU
// since 2010 should work fine. For 64-bit atomics on ARM CPUs, the instruction set should be at least ARMv8.1-A.
// Earlier instruction sets also work, but might yield less efficient code. More information can be found here:
// https://community.arm.com/arm-community-blogs/b/tools-software-ides-blog/posts/making-the-most-of-the-arm-architecture-in-gcc-10  // NOLINT
STRONG_TYPEDEF(uint32_t, CommitID);
STRONG_TYPEDEF(uint32_t, TransactionID);

// Used to identify a Parameter within a subquery. This can be either a parameter of a Prepared SELECT statement
// `SELECT * FROM t WHERE a > ?` or a correlated parameter in a subquery.
STRONG_TYPEDEF(uint16_t, ParameterID);

namespace hyrise {

// Float aliases used in cardinality estimations/statistics
using Cardinality = float;
using DistinctCount = float;
using Selectivity = float;

// Cost that an AbstractCostModel assigns to an Operator/LQP node. The unit of the Cost is left to the Cost estimator
// and could be, e.g., "Estimated Runtime" or "Estimated Memory Usage" (though the former is by far the most common)
using Cost = float;

// We use polymorphic memory resources to allow containers (e.g., vectors, or strings) to retrieve their memory from
// different memory sources. These sources are, for example, specific NUMA nodes or non-volatile memory. Without PMR,
// we would need to explicitly make the allocator part of the class. This would make DRAM and NVM containers type-
// incompatible. Thanks to PMR, the type is erased and both can co-exist.
//
// TODO(anyone): replace this with std::pmr once libc++ supports PMR.
template <typename T>
using PolymorphicAllocator = boost::container::pmr::polymorphic_allocator<T>;

// The string type that is used internally to store data. It's hard to draw the line between this and std::string or
// give advice when to use what. Generally, everything that is user-supplied data (mostly, data stored in a table) is a
// pmr_string. Also, the string literals in SQL queries will get converted into a pmr_string (and then stored in an
// AllTypeVariant). This way, they can be compared to the pmr_string stored in the table. Strings that are built, e.g.,
// for debugging, do not need to use PMR. This might sound complicated, but since the Hyrise data type registered in
// all_type_variant.hpp is pmr_string, the compiler will complain if you use std::string when you should use pmr_string.
using pmr_string = std::basic_string<char, std::char_traits<char>, PolymorphicAllocator<char>>;

// A vector that gets its memory from a memory resource. It is is not necessary to replace each and every std::vector
// with this. It only makes sense to use this if you also supply a memory resource. Otherwise, default memory will be
// used and we do not gain anything but have minimal runtime overhead. As a side note, PMR propagates, so a
// `pmr_vector<pmr_string>` will pass its memory resource down to the strings while a `pmr_vector<std::string>` will
// allocate the space for the vector at the correct location while the content of the strings will be in default
// storage.
// Note that a container initialized with a given allocator will keep that allocator, even if it is copy/move assigned:
//   pmr_vector<int> a, b{alloc};
//   a = b;  // a does NOT use alloc, neither for its current values, nor for future allocations (#623).
template <typename T>
using pmr_vector = std::vector<T, PolymorphicAllocator<T>>;

template <typename T>
using pmr_ring_buffer = boost::circular_buffer<T, PolymorphicAllocator<T>>;

constexpr ChunkOffset INVALID_CHUNK_OFFSET{std::numeric_limits<ChunkOffset::base_type>::max()};
constexpr ChunkID INVALID_CHUNK_ID{std::numeric_limits<ChunkID::base_type>::max()};

struct RowID {
  ChunkID chunk_id{INVALID_CHUNK_ID};
  ChunkOffset chunk_offset{INVALID_CHUNK_OFFSET};

  // Faster than row_id == ROW_ID_NULL, since we only compare the ChunkOffset
  bool is_null() const {
    return chunk_offset == INVALID_CHUNK_OFFSET;
  }

  // Joins need to use RowIDs as keys for maps.
  bool operator<(const RowID& other) const {
    return std::tie(chunk_id, chunk_offset) < std::tie(other.chunk_id, other.chunk_offset);
  }

  // Useful when comparing a row ID to NULL_ROW_ID
  bool operator==(const RowID& other) const {
    return std::tie(chunk_id, chunk_offset) == std::tie(other.chunk_id, other.chunk_offset);
  }

  friend std::ostream& operator<<(std::ostream& stream, const RowID& row_id) {
    stream << "RowID(" << row_id.chunk_id << "," << row_id.chunk_offset << ")";
    return stream;
  }
};

using CompressedVectorTypeID = uint8_t;

using ColumnIDPair = std::pair<ColumnID, ColumnID>;

constexpr NodeID INVALID_NODE_ID{std::numeric_limits<NodeID::base_type>::max()};
constexpr TaskID INVALID_TASK_ID{std::numeric_limits<TaskID::base_type>::max()};
constexpr CpuID INVALID_CPU_ID{std::numeric_limits<CpuID::base_type>::max()};
constexpr WorkerID INVALID_WORKER_ID{std::numeric_limits<WorkerID::base_type>::max()};
constexpr ColumnID INVALID_COLUMN_ID{std::numeric_limits<ColumnID::base_type>::max()};

// TransactionID = 0 means "not set" in the MVCC data. This is the case if the row has (a) just been reserved, but not
// yet filled with content, (b) been inserted, committed and not marked for deletion, or (c) inserted but deleted in
// the same transaction (which has not yet committed)
constexpr auto INVALID_TRANSACTION_ID = TransactionID{0};
constexpr auto INITIAL_TRANSACTION_ID = TransactionID{1};

constexpr NodeID CURRENT_NODE_ID{std::numeric_limits<NodeID::base_type>::max() - 1};

// Declaring one part of a RowID as invalid would suffice to represent NULL values. However, this way we add an extra
// safety net which ensures that NULL values are handled correctly. E.g., getting a chunk with INVALID_CHUNK_ID
// immediately crashes.
constexpr RowID NULL_ROW_ID = RowID{INVALID_CHUNK_ID, INVALID_CHUNK_OFFSET};

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
  BetweenInclusive,
  BetweenLowerExclusive,
  BetweenUpperExclusive,
  BetweenExclusive,
  In,
  NotIn,
  Like,
  NotLike,
  IsNull,
  IsNotNull
};

// @return whether the PredicateCondition takes exactly two arguments
bool is_binary_predicate_condition(const PredicateCondition predicate_condition);

// @return whether the PredicateCondition takes exactly two arguments and is not one of LIKE or IN
bool is_binary_numeric_predicate_condition(const PredicateCondition predicate_condition);

bool is_between_predicate_condition(PredicateCondition predicate_condition);

bool is_lower_inclusive_between(PredicateCondition predicate_condition);

bool is_upper_inclusive_between(PredicateCondition predicate_condition);

// ">" becomes "<" etc.
PredicateCondition flip_predicate_condition(const PredicateCondition predicate_condition);

// ">" becomes "<=" etc.
PredicateCondition inverse_predicate_condition(const PredicateCondition predicate_condition);

// Split up, e.g., BetweenUpperExclusive into {GreaterThanEquals, LessThan}
std::pair<PredicateCondition, PredicateCondition> between_to_conditions(const PredicateCondition predicate_condition);

// Join, e.g., {GreaterThanEquals, LessThan} into BetweenUpperExclusive
PredicateCondition conditions_to_between(const PredicateCondition lower, const PredicateCondition upper);

// Let R and S be two tables and we want to perform `R <JoinMode> S ON <condition>`
// AntiNullAsTrue:    If for a tuple Ri in R, there is a tuple Sj in S so that <condition> is NULL or TRUE, Ri is
//                      dropped. This behavior mirrors NOT IN.
// AntiNullAsFalse:   If for a tuple Ri in R, there is a tuple Sj in S so that <condition> is TRUE, Ri is
//                      dropped. This behavior mirrors NOT EXISTS
enum class JoinMode { Inner, Left, Right, FullOuter, Cross, Semi, AntiNullAsTrue, AntiNullAsFalse };

// SQL set operations come in two flavors, with and without `ALL`, e.g., `UNION` and `UNION ALL`.
// We have a third mode (Positions) that is used to intersect position lists that point to the same table,
// see union_positions.hpp for details.
enum class SetOperationMode { Unique, All, Positions };

// According to the SQL standard, the position of NULLs is implementation-defined. In Hyrise, NULLs come before all
// values, both for ascending and descending sorts. See sort.cpp for details.
enum class SortMode { Ascending, Descending };

enum class TableType { References, Data };

enum class DescriptionMode { SingleLine, MultiLine };

enum class UseMvcc : bool { Yes = true, No = false };

enum class RollbackReason : bool { User, Conflict };

enum class MemoryUsageCalculationMode { Sampled, Full };

enum class EraseReferencedSegmentType : bool { Yes = true, No = false };

enum class MetaTableChangeType { Insert, Delete, Update };

enum class AutoCommit : bool { Yes = true, No = false };

enum class LogLevel { Debug, Info, Warning };

enum class DatetimeComponent { Year, Month, Day, Hour, Minute, Second };

// Used as a template parameter that is passed whenever we conditionally erase the type of a template. This is done to
// reduce the compile time at the cost of the runtime performance. Examples are iterators, which are replaced by
// AnySegmentIterators that use virtual method calls.
enum class EraseTypes { OnlyInDebugBuild, Always };

// Defines in which order a certain column should be or is sorted.
struct SortColumnDefinition final {
  explicit SortColumnDefinition(ColumnID init_column, SortMode init_sort_mode = SortMode::Ascending)
      : column(init_column), sort_mode(init_sort_mode) {}

  ColumnID column;
  SortMode sort_mode;
};

inline bool operator==(const SortColumnDefinition& lhs, const SortColumnDefinition& rhs) {
  return lhs.column == rhs.column && lhs.sort_mode == rhs.sort_mode;
}

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

extern const boost::bimap<PredicateCondition, std::string> predicate_condition_to_string;
extern const boost::bimap<SortMode, std::string> sort_mode_to_string;
extern const boost::bimap<JoinMode, std::string> join_mode_to_string;
extern const boost::bimap<SetOperationMode, std::string> set_operation_mode_to_string;
extern const boost::bimap<TableType, std::string> table_type_to_string;

std::ostream& operator<<(std::ostream& stream, PredicateCondition predicate_condition);
std::ostream& operator<<(std::ostream& stream, SortMode sort_mode);
std::ostream& operator<<(std::ostream& stream, JoinMode join_mode);
std::ostream& operator<<(std::ostream& stream, SetOperationMode set_operation_mode);
std::ostream& operator<<(std::ostream& stream, TableType table_type);

using BoolAsByteType = uint8_t;

}  // namespace hyrise

namespace std {
// The hash method for pmr_string (see above). We explicitly don't use the alias here as this allows us to write
// `using pmr_string = std::string` above. If we had `pmr_string` here, we would try to redefine an existing hash
// function.
template <>
struct hash<std::basic_string<char, std::char_traits<char>, hyrise::PolymorphicAllocator<char>>> {
  size_t operator()(
      const std::basic_string<char, std::char_traits<char>, hyrise::PolymorphicAllocator<char>>& string) const {
    return std::hash<std::string_view>{}(string.c_str());
  }
};
}  // namespace std
