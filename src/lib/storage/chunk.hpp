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

namespace opossum {

class AbstractIndex;
class AbstractSegment;
class BaseAttributeStatistics;

using Segments = pmr_vector<std::shared_ptr<AbstractSegment>>;
using Indexes = pmr_vector<std::shared_ptr<AbstractIndex>>;
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
  // This is the architecture-defined limit on the size of a single chunk. The last chunk offset is reserved for NULL
  // as used in ReferenceSegments.
  static constexpr ChunkOffset MAX_SIZE = std::numeric_limits<ChunkOffset>::max() - 1;

  // For a new chunk, this is the size of the pre-allocated ValueSegments. This is only relevant for chunks that
  // contain data. Chunks that contain reference segments do not use the table's target_chunk_size at all.
  //
  // The default chunk size was determined to give the best performance for single-threaded TPC-H, SF1. By all means,
  // feel free to re-evaluate this. 2^16 is a good size because it means that on a unique column, dictionary
  // requires up to 16 bits for the value ids. A chunk size of 100'000 would put us just slightly over that 16 bits,
  // meaning that FixedWidthInteger vectors would use 32 instead of 16 bits. We do not use 65'536 because we need to
  // account for NULL being encoded as a separate value id.
  static constexpr ChunkOffset DEFAULT_SIZE = 65'535;

  Chunk(Segments segments, const std::shared_ptr<MvccData>& mvcc_data = nullptr,
        const std::optional<PolymorphicAllocator<Chunk>>& alloc = std::nullopt, Indexes indexes = {});

  // Returns whether new rows can be appended to this Chunk. Chunks are set immutable during finalize().
  bool is_mutable() const;

  // Atomically replaces the current segment at column_id with the passed segment
  void replace_segment(size_t column_id, const std::shared_ptr<AbstractSegment>& segment);

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
  std::shared_ptr<AbstractSegment> get_segment(ColumnID column_id) const;

  bool has_mvcc_data() const;

  std::shared_ptr<MvccData> mvcc_data() const;

  std::vector<std::shared_ptr<AbstractIndex>> get_indexes(
      const std::vector<std::shared_ptr<const AbstractSegment>>& segments) const;
  std::vector<std::shared_ptr<AbstractIndex>> get_indexes(const std::vector<ColumnID>& column_ids) const;

  std::shared_ptr<AbstractIndex> get_index(const SegmentIndexType index_type,
                                           const std::vector<std::shared_ptr<const AbstractSegment>>& segments) const;
  std::shared_ptr<AbstractIndex> get_index(const SegmentIndexType index_type,
                                           const std::vector<ColumnID>& column_ids) const;

  template <typename Index>
  std::shared_ptr<AbstractIndex> create_index(
      const std::vector<std::shared_ptr<const AbstractSegment>>& segments_to_index) {
    DebugAssert(([&]() {
                  for (auto segment : segments_to_index) {
                    const auto segment_it = std::find(_segments.cbegin(), _segments.cend(), segment);
                    if (segment_it == _segments.cend()) return false;
                  }
                  return true;
                }()),
                "All segments must be part of the chunk.");

    auto index = std::make_shared<Index>(segments_to_index);
    _indexes.emplace_back(index);
    return index;
  }

  template <typename Index>
  std::shared_ptr<AbstractIndex> create_index(const std::vector<ColumnID>& column_ids) {
    const auto segments = _get_segments_for_ids(column_ids);
    return create_index<Index>(segments);
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
   * If a chunk is sorted in any way, the sort mode (Ascending/Descending) and the ColumnIDs of the segments by which
   * it is sorted will be returned.
   *
   * In a chunk, multiple segments may be sorted independently. For example, in a table storing orders, both the order
   * id and date of incoming orders might have incrementing values. In this case, sorted_by has two entries (assuming
   * this knowledge is available to the database). However, for cases where the data is first orderered by one column,
   * then by another (e.g. ORDER_BY last_name, first_name), only the primary order is stored.
   *
   * Sort orders are currently exploited in several scan implementations (e.g., ColumnVsValue, ColumnIsNull,
   * ColumnBetweenScan) and selected other operators (e.g., AggregateSort). See #1519 for more details.
   */
  const std::vector<SortColumnDefinition>& individually_sorted_by() const;
  void set_individually_sorted_by(const SortColumnDefinition& sorted_by);
  void set_individually_sorted_by(const std::vector<SortColumnDefinition>& sorted_by);

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
   * Executes tasks that are connected with finalizing a chunk. Currently, chunks are made immutable, and
   * depending on skip_mvcc_check, the MVCC max_begin_cid is set. Finalizing a chunk is the inserter's responsibility.
   */
  void finalize();

 private:
  std::vector<std::shared_ptr<const AbstractSegment>> _get_segments_for_ids(
      const std::vector<ColumnID>& column_ids) const;

 private:
  PolymorphicAllocator<Chunk> _alloc;
  Segments _segments;
  std::shared_ptr<MvccData> _mvcc_data;
  Indexes _indexes;
  std::optional<ChunkPruningStatistics> _pruning_statistics;
  bool _is_mutable = true;
  std::vector<SortColumnDefinition> _sorted_by;
  mutable std::atomic<ChunkOffset> _invalid_row_count{0};

  // Default value of zero means "not set"
  std::atomic<CommitID> _cleanup_commit_id{0};
  static_assert(std::is_same<uint32_t, CommitID>::value, "Type of _cleanup_commit_id does not match type of CommitID.");
};

}  // namespace opossum
