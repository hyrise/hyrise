#pragma once

#include <tbb/concurrent_vector.h>
#include <boost/container/pmr/memory_resource.hpp>

// the linter wants this to be above everything else
#include <shared_mutex>

#include <algorithm>
#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "index/segment_index_type.hpp"

#include "all_type_variant.hpp"
#include "mvcc_data.hpp"
#include "table_column_definition.hpp"
#include "types.hpp"
#include "utils/copyable_atomic.hpp"
#include "utils/scoped_locking_ptr.hpp"

namespace opossum {

class BaseIndex;
class BaseSegment;
class ChunkStatistics;

using Segments = pmr_vector<std::shared_ptr<BaseSegment>>;

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

  Chunk(const Segments& segments, const std::shared_ptr<MvccData>& mvcc_data = nullptr,
        const std::optional<PolymorphicAllocator<Chunk>>& alloc = std::nullopt);

  // returns whether new rows can be appended to this Chunk
  bool is_mutable() const;

  void mark_immutable();

  // Atomically replaces the current segment at column_id with the passed segment
  void replace_segment(size_t column_id, const std::shared_ptr<BaseSegment>& segment);

  // returns the number of columns, which is equal to the number of segments (cannot exceed ColumnID (uint16_t))
  uint16_t column_count() const;

  // returns the number of rows (cannot exceed ChunkOffset (uint32_t))
  uint32_t size() const;

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
  std::shared_ptr<BaseSegment> get_segment(ColumnID column_id) const;

  const Segments& segments() const;

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
  void set_mvcc_data(const std::shared_ptr<MvccData>& mvcc_data);

  std::vector<std::shared_ptr<BaseIndex>> get_indices(
      const std::vector<std::shared_ptr<const BaseSegment>>& segments) const;
  std::vector<std::shared_ptr<BaseIndex>> get_indices(const std::vector<ColumnID>& column_ids) const;

  std::shared_ptr<BaseIndex> get_index(const SegmentIndexType index_type,
                                       const std::vector<std::shared_ptr<const BaseSegment>>& segments) const;
  std::shared_ptr<BaseIndex> get_index(const SegmentIndexType index_type,
                                       const std::vector<ColumnID>& column_ids) const;

  template <typename Index>
  std::shared_ptr<BaseIndex> create_index(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index) {
    DebugAssert(([&]() {
                  for (auto segment : segments_to_index) {
                    const auto segment_it = std::find(_segments.cbegin(), _segments.cend(), segment);
                    if (segment_it == _segments.cend()) return false;
                  }
                  return true;
                }()),
                "All segments must be part of the chunk.");

    auto index = std::make_shared<Index>(segments_to_index);
    _indices.emplace_back(index);
    return index;
  }

  template <typename Index>
  std::shared_ptr<BaseIndex> create_index(const std::vector<ColumnID>& column_ids) {
    const auto segments = _get_segments_for_ids(column_ids);
    return create_index<Index>(segments);
  }

  void remove_index(const std::shared_ptr<BaseIndex>& index);

  void migrate(boost::container::pmr::memory_resource* memory_source);

  bool references_exactly_one_table() const;

  const PolymorphicAllocator<Chunk>& get_allocator() const;

  std::shared_ptr<ChunkStatistics> statistics() const;

  void set_statistics(const std::shared_ptr<ChunkStatistics>& chunk_statistics);

  /**
   * For debugging purposes, makes an estimation about the memory used by this chunk and its segments
   */
  size_t estimate_memory_usage() const;

 private:
  std::vector<std::shared_ptr<const BaseSegment>> _get_segments_for_ids(const std::vector<ColumnID>& column_ids) const;

 private:
  PolymorphicAllocator<Chunk> _alloc;
  Segments _segments;
  std::shared_ptr<MvccData> _mvcc_data;
  pmr_vector<std::shared_ptr<BaseIndex>> _indices;
  std::shared_ptr<ChunkStatistics> _statistics;
  bool _is_mutable = true;
};

}  // namespace opossum
