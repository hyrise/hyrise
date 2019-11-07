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
STRONG_TYPEDEF(opossum::ColumnID::base_type, ColumnCount);
STRONG_TYPEDEF(uint32_t, ValueID);  // Cannot be larger than ChunkOffset
STRONG_TYPEDEF(uint32_t, NodeID);
STRONG_TYPEDEF(uint32_t, CpuID);

// Used to identify a Parameter within a subquery. This can be either a parameter of a Prepared SELECT statement
// `SELECT * FROM t WHERE a > ?` or a correlated parameter in a subquery.
STRONG_TYPEDEF(size_t, ParameterID);

namespace opossum {

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

// TransactionID = 0 means "not set" in the MVCC data. This is the case if the row has (a) just been reserved, but
// not yet filled with content, (b) been inserted, committed and not marked for deletion, or (c) inserted but
// deleted in the same transaction (which has not yet committed)
constexpr auto INVALID_TRANSACTION_ID = TransactionID{0};
constexpr auto INITIAL_TRANSACTION_ID = TransactionID{1};

constexpr NodeID CURRENT_NODE_ID{std::numeric_limits<NodeID::base_type>::max() - 1};

// Declaring one part of a RowID as invalid would suffice to represent NULL values. However, this way we add an extra
// safety net which ensures that NULL values are handled correctly. E.g., getting a chunk with INVALID_CHUNK_ID
// immediately crashes.
const RowID NULL_ROW_ID = RowID{INVALID_CHUNK_ID, INVALID_CHUNK_OFFSET};  // TODO(anyone): Couldn’t use constexpr here

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

enum class UnionMode { Positions, All };

enum class OrderByMode { Ascending, Descending, AscendingNullsLast, DescendingNullsLast };

enum class TableType { References, Data };

enum class DescriptionMode { SingleLine, MultiLine };

enum class UseMvcc : bool { Yes = true, No = false };

enum class CleanupTemporaries : bool { Yes = true, No = false };

enum class HasNullTerminator : bool { Yes = true, No = false };

enum class SendExecutionInfo : bool { Yes = true, No = false };

// Used as a template parameter that is passed whenever we conditionally erase the type of a template. This is done to
// reduce the compile time at the cost of the runtime performance. Examples are iterators, which are replaced by
// AnySegmentIterators that use virtual method calls.
enum class EraseTypes { OnlyInDebugBuild, Always };

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
extern const boost::bimap<OrderByMode, std::string> order_by_mode_to_string;
extern const boost::bimap<JoinMode, std::string> join_mode_to_string;
extern const boost::bimap<UnionMode, std::string> union_mode_to_string;
extern const boost::bimap<TableType, std::string> table_type_to_string;

std::ostream& operator<<(std::ostream& stream, PredicateCondition predicate_condition);
std::ostream& operator<<(std::ostream& stream, OrderByMode order_by_mode);
std::ostream& operator<<(std::ostream& stream, JoinMode join_mode);
std::ostream& operator<<(std::ostream& stream, UnionMode union_mode);
std::ostream& operator<<(std::ostream& stream, TableType table_type);

}  // namespace opossum

namespace std {
// The hash method for pmr_string (see above). We explicitly don't use the alias here as this allows us to write
// `using pmr_string = std::string` above. If we had `pmr_string` here, we would try to redefine an existing hash
// function.
template <>
struct hash<std::basic_string<char, std::char_traits<char>, opossum::PolymorphicAllocator<char>>> {
  size_t operator()(
      const std::basic_string<char, std::char_traits<char>, opossum::PolymorphicAllocator<char>>& string) const {
    return std::hash<std::string_view>{}(string.c_str());
  }
};
}  // namespace std
