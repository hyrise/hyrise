#pragma once

#include <tbb/concurrent_vector.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include <boost/container/pmr/memory_resource.hpp>

#include "all_type_variant.hpp"
#include "index/segment_index_type.hpp"
#include "mvcc_data.hpp"
#include "table_column_definition.hpp"
#include "types.hpp"
#include "utils/copyable_atomic.hpp"
#include "utils/scoped_locking_ptr.hpp"

namespace opossum {

class AbstractIndex;
class BaseSegment;
class BaseAttributeStatistics;

using Segments = pmr_vector<std::shared_ptr<BaseSegment>>;
using IndexesByColumnIDsMap = std::map<std::vector<ColumnID>, std::shared_ptr<AbstractIndex>>;
using ChunkPruningStatistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>;

/**
 * A Chunk is a horizontal partition of a table.
 * It stores the table's data segment by segment.
 * Optionally, mostly applying to StoredTables, it may also hold MvccData.
 *
 * Find more information about this in our wiki: https://github.com/hyrise/hyrise/wiki/chunk-concept
 */
class Chunk : private Noncopyable {
 public:
  // The last chunk offset is reserved for NULL as used in ReferenceSegments.
  static constexpr ChunkOffset MAX_SIZE = std::numeric_limits<ChunkOffset>::max() - 1;

  // The default chunk size was determined to give the best performance for single-threaded TPC-H, SF1. By all means,
  // feel free to re-evaluate this. This is only relevant for chunks that contain data. Chunks that contain reference
  // segments do not use the table's max_chunk_size at all.
  static constexpr ChunkOffset DEFAULT_SIZE = 100'000;

  // Index vector is a shared ptr to a vector as we need to ensure that the list of vectors is not destroyed during
  // query execution. We cannot pass references here, FOR TODO reason
  // TODO:
  Chunk(Segments segments, const std::shared_ptr<MvccData>& mvcc_data = nullptr,
        const std::optional<PolymorphicAllocator<Chunk>>& alloc = std::nullopt,
        std::shared_ptr<IndexesByColumnIDsMap> indexes = std::make_shared<IndexesByColumnIDsMap>());

  // Returns whether new rows can be appended to this Chunk. Chunks are set immutable during finalize().
  bool is_mutable() const;

  // Atomically replaces the current segment at column_id with the passed segment
  void replace_segment(size_t column_id, const std::shared_ptr<BaseSegment>& segment);

  // returns the number of columns, which is equal to the number of segments (cannot exceed ColumnID (uint16_t))
  ColumnCount column_count() const;

  // returns the number of rows (cannot exceed ChunkOffset (uint32_t))
  ChunkOffset size() const;

  // adds a new row, given as a list of values, to the chunk
  // note this is slow and not thread-safe and should be used for testing purposes only
  void append(const std::vector<AllTypeVariant>& values);

  /**
   * Atomically accesses and returns the segment at a given position
   *
   * Note: Concurrently with the execution of operators,
   *       ValueSegments might be exchanged with DictionarySegments.
   *       Therefore, if you hold a pointer to a segment, you can
   *       continue to use it without any inconsistencies.
   *       However, if you call get_segment again, be aware that
   *       the return type might have changed.
   */
  std::shared_ptr<BaseSegment> get_segment(const ColumnID column_id) const;

  bool has_mvcc_data() const;

  /**
   * The locking pointer locks the MVCC data non-exclusively
   * and unlocks them on destruction
   *
   * For improved performance, it is best to call this function
   * once and retain the reference as long as needed.
   *
   * @return a locking ptr to the MVCC data
   */
  SharedScopedLockingPtr<MvccData> get_scoped_mvcc_data_lock() const;

  std::shared_ptr<MvccData> mvcc_data() const;

  const std::shared_ptr<IndexesByColumnIDsMap>& indexes() const;

  /**
   * With the current status of indexes in Hyrise, we have either single column indexes (trivially to handle) and
   * multi-column indexes which make certain assumptions. E.g., they have to accessed with the the same order of
   * columns as they store internally. Further, while not all indexed columns of the index have to be accessed for
   * searches, the accessed ones have to be all in the front of the covered columns of the index.

   * We store indexes per chunk in a vector of pairs with the covered column IDs and an abstract index shared pointer.

   * Performance: we do not use maps or other data structures here. The reasons are, first, that they are not of much
   * help for multi-index searches (multi-maps with intersections are possible but expensive) and, second, we expect
   * the number of indexes and the number of covered columns in multi-column indexes to be rather small (i.e.,
   * usually not more than half a dozen). Iterating vectors and iteratively checking their applicability is thus
   * sufficiently fast that a worst case complexity of m*n.
   *
   * We only return indexes whose columns cover all requested columns, even though it can make sense to use a more
   * narrow index and apply the remaining column filter in a following table scan.
   */
  const std::vector<std::shared_ptr<AbstractIndex>> applicable_indexes(const std::vector<const ColumnID> column_ids);

  // TODO: move this method into createIndex(columnIDs)
  template <typename Index>
  std::shared_ptr<AbstractIndex> create_index(
      const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index) {
    DebugAssert(([&]() {
                  for (auto segment : segments_to_index) {
                    const auto segment_it = std::find(_segments.cbegin(), _segments.cend(), segment);
                    if (segment_it == _segments.cend()) return false;
                  }
                  return true;
                }()),
                "All segments must be part of the chunk.");

    auto index = std::make_shared<Index>(segments_to_index);
    return index;
  }

  template <typename Index>
  std::shared_ptr<AbstractIndex> create_index(const std::vector<ColumnID> column_ids) {
    const auto segments = _get_segments_for_ids(column_ids);
    const auto index = create_index<Index>(segments);
    _indexes->emplace(std::move(column_ids), index);
    return index;
  }

  void remove_index(const std::shared_ptr<AbstractIndex>& index);

  void migrate(boost::container::pmr::memory_resource* memory_source);

  bool references_exactly_one_table() const;

  const PolymorphicAllocator<Chunk>& get_allocator() const;

  /** 
   * To perform Chunk pruning, a Chunk can be associated with statistics.
   * @{
   */
  const std::optional<ChunkPruningStatistics>& pruning_statistics() const;
  void set_pruning_statistics(const std::optional<ChunkPruningStatistics>& pruning_statistics);
  /** @} */

  /**
   * For debugging purposes, makes an estimation about the memory used by this chunk and its segments
   */
  size_t memory_usage(const MemoryUsageCalculationMode mode) const;

  /**
   * If a chunk is sorted in any way, the order (Ascending/Descending/AscendingNullsFirst/AscendingNullsLast) and
   * the ColumnID of the segment by which it is sorted will be returned.
   * This is currently only taken advantage of in the ColumnVsValueScan. See #1519 for more details.
   */
  const std::optional<std::pair<ColumnID, OrderByMode>>& ordered_by() const;
  void set_ordered_by(const std::pair<ColumnID, OrderByMode>& ordered_by);

  /**
   * Returns the count of deleted/invalidated rows within this chunk resulting from already committed transactions.
   * However, `size() - invalid_row_count()` does not necessarily tell you how many rows are visible for
   * the current transaction.
   */
  ChunkOffset invalid_row_count() const { return _invalid_row_count.load(); }

  /**
   * Atomically increases the counter of deleted/invalidated rows within this chunk.
   * (The function is marked as const, as otherwise it could not be called by the Delete operator.)
   */
  void increase_invalid_row_count(ChunkOffset count) const;

  /**
   * Chunks with few visible entries can be cleaned up periodically by the MvccDeletePlugin in a two-step process.
   * Within the first step (clean up transaction), the plugin deletes rows from this chunk and re-inserts them at the
   * end of the table. Thus, future transactions will find the still valid rows at the end of the table and do not
   * have to look at this chunk anymore.
   * The cleanup commit id represents the snapshot commit id at which transactions can ignore this chunk.
   */
  std::optional<CommitID> get_cleanup_commit_id() const;

  void set_cleanup_commit_id(CommitID cleanup_commit_id);

  /**
   * Executes tasks that are connected with finalizing a chunk. Currently, chunks are made immutable and
   * the MVCC max_begin_cid is set. Finalizing a chunk is the inserter's responsibility.
   */
  void finalize();

 private:
  std::vector<std::shared_ptr<const BaseSegment>> _get_segments_for_ids(const std::vector<ColumnID>& column_ids) const;

 private:
  PolymorphicAllocator<Chunk> _alloc;
  Segments _segments;
  std::shared_ptr<MvccData> _mvcc_data;
  std::optional<ChunkPruningStatistics> _pruning_statistics;
  bool _is_mutable = true;
  std::optional<std::pair<ColumnID, OrderByMode>> _ordered_by;
  mutable std::atomic<ChunkOffset> _invalid_row_count{0};

  // std::map over std::unordered_map as we do not need to implement hashing of vectors the map is expected to be small.
  std::shared_ptr<IndexesByColumnIDsMap> _indexes;

  // Default value of zero means "not set"
  std::atomic<CommitID> _cleanup_commit_id{0};
  static_assert(std::is_same<uint32_t, CommitID>::value, "Type of _cleanup_commit_id does not match type of CommitID.");
};

}  // namespace opossum
