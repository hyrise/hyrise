#include "insert.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/atomic_max.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

template <typename T>
void copy_value_range(const std::shared_ptr<const AbstractSegment>& source_abstract_segment,
                      ChunkOffset source_begin_offset, const std::shared_ptr<AbstractSegment>& target_abstract_segment,
                      ChunkOffset target_begin_offset, ChunkOffset length) {
  DebugAssert(source_abstract_segment->size() >= source_begin_offset + length, "Source Segment out-of-bounds.");
  DebugAssert(target_abstract_segment->size() >= target_begin_offset + length, "Target Segment out-of-bounds.");

  const auto target_value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(target_abstract_segment);
  Assert(target_value_segment, "Cannot insert into non-ValueSegments.");

  auto& target_values = target_value_segment->values();

  /**
   * If the source Segment is a ValueSegment, take a fast path to copy the data. Otherwise, take a (potentially slower)
   * fallback path.
   */
  if (const auto source_value_segment = std::dynamic_pointer_cast<const ValueSegment<T>>(source_abstract_segment)) {
    std::copy_n(source_value_segment->values().begin() + source_begin_offset, length,
                target_values.begin() + target_begin_offset);

    if (source_value_segment->is_nullable()) {
      const auto nulls_begin_iter = source_value_segment->null_values().begin() + source_begin_offset;
      const auto nulls_end_iter = nulls_begin_iter + length;

      auto nulls_target_offset = target_begin_offset;
      for (auto nulls_iter = nulls_begin_iter; nulls_iter != nulls_end_iter; ++nulls_iter) {
        if (*nulls_iter) {
          target_value_segment->set_null_value(nulls_target_offset);
        }
        ++nulls_target_offset;
      }
    }
  } else {
    segment_with_iterators<T>(*source_abstract_segment, [&](const auto& source_begin, const auto& /*source_end*/) {
      auto source_iter = source_begin + source_begin_offset;
      auto target_iter = target_values.begin() + target_begin_offset;

      // Copy values and NULL values.
      for (auto index = ChunkOffset{0}; index < length; ++index) {
        *target_iter = source_iter->value();

        if (source_iter->is_null()) {
          // ValueSegments not being NULLable will be handled over there.
          target_value_segment->set_null_value(ChunkOffset{target_begin_offset + index});
        }

        ++source_iter;
        ++target_iter;
      }
    });
  }
}

}  // namespace

namespace hyrise {

Insert::Insert(const std::string& target_table_name, const std::shared_ptr<const AbstractOperator>& values_to_insert)
    : AbstractReadWriteOperator(OperatorType::Insert, values_to_insert), _target_table_name(target_table_name) {}

const std::string& Insert::name() const {
  static const auto name = std::string{"Insert"};
  return name;
}

std::shared_ptr<const Table> Insert::_on_execute(std::shared_ptr<TransactionContext> context) {
  _target_table = Hyrise::get().storage_manager.get_table(_target_table_name);

  Assert(_target_table->target_chunk_size() > 0, "Expected target chunk size of target table to be greater than zero.");
  for (auto column_id = ColumnID{0}; column_id < _target_table->column_count(); ++column_id) {
    // This is not really a strong limitation, we just did not want the compile time of all type combinations. If you
    // really want this, it should be only a couple of lines to implement.
    Assert(left_input_table()->column_data_type(column_id) == _target_table->column_data_type(column_id),
           "Cannot handle inserts into column of different type.");
  }

  /**
   * 1. Allocate the required rows in the target Table, without actually copying data to them.  Do so while locking the
   *    table to prevent multiple threads modifying the table's size simultaneously. Since allocation is expected to be
   *    faster than writing to the memory, allocating under lock and then writing - in a second step - without lock will
   *    minimize the time that the Table's `_append_mutex` is locked.
   */
  {
    const auto append_lock = _target_table->acquire_append_mutex();

    auto remaining_rows = left_input_table()->row_count();

    if (_target_table->chunk_count() == 0) {
      _target_table->append_mutable_chunk();
    }
    while (remaining_rows > 0) {
      auto target_chunk_id = ChunkID{_target_table->chunk_count() - 1};
      auto target_chunk = _target_table->get_chunk(target_chunk_id);
      const auto target_size = _target_table->target_chunk_size();

      // If the last chunk of the target table is either immutable or full, append a new mutable chunk.
      if (!target_chunk->is_mutable() || target_chunk->size() == target_size) {
        _target_table->append_mutable_chunk();
        ++target_chunk_id;
        target_chunk = _target_table->get_chunk(target_chunk_id);
      }

      // Register that Insert is pending. See `chunk.hpp`. for details.
      const auto& mvcc_data = target_chunk->mvcc_data();
      DebugAssert(mvcc_data, "Insert cannot operate on a table without MVCC data.");
      mvcc_data->register_insert();

      const auto num_rows_for_target_chunk =
          std::min<size_t>(_target_table->target_chunk_size() - target_chunk->size(), remaining_rows);

      _target_chunk_ranges.emplace_back(
          ChunkRange{target_chunk_id, target_chunk->size(),
                     static_cast<ChunkOffset>(target_chunk->size() + num_rows_for_target_chunk)});

      // Mark new (but still empty) rows as being under modification by current transaction. Do so before resizing the
      // Segments, because the resize of `Chunk::_segments.front()` is what releases the new row count.
      {
        const auto transaction_id = context->transaction_id();
        const auto end_offset = target_chunk->size() + num_rows_for_target_chunk;
        for (auto target_chunk_offset = target_chunk->size(); target_chunk_offset < end_offset; ++target_chunk_offset) {
          DebugAssert(mvcc_data->get_begin_cid(target_chunk_offset) == MAX_COMMIT_ID, "Invalid begin CID.");
          DebugAssert(mvcc_data->get_end_cid(target_chunk_offset) == MAX_COMMIT_ID, "Invalid end CID.");
          mvcc_data->set_tid(target_chunk_offset, transaction_id, std::memory_order_relaxed);
        }
      }

      // Make sure the MVCC data is written before the first segment (and, thus, the chunk) is resized.
      std::atomic_thread_fence(std::memory_order_seq_cst);

      // "Grow" data segments: Segments are pre-allocated during construction. We resize() to make default-constructed
      // cells visible so that they can be written in the next Insert step. Note that those cells are still invisible
      // from an MVCC point of view until they are actually being written and committed.
      // Do so in REVERSE column order so that the resize of `Chunk::_segments.front()` happens last. It is this last
      // resize that makes the new row count visible to the outside world.
      const auto old_size = target_chunk->size();
      const auto new_size = old_size + num_rows_for_target_chunk;
      const auto column_count = target_chunk->column_count();
      for (auto reverse_column_id = ColumnID{0}; reverse_column_id < column_count; ++reverse_column_id) {
        const auto column_id = static_cast<ColumnID>(column_count - reverse_column_id - 1);

        resolve_data_type(_target_table->column_data_type(column_id), [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;

          const auto value_segment =
              std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(target_chunk->get_segment(column_id));
          Assert(value_segment, "Cannot insert into non-ValueSegments.");

          // Cannot guarantee resize without reallocation when growing. We thus check that the ValueSegment has been
          // allocated with the target table's target chunk size reserved.
          Assert(value_segment->values().capacity() >= new_size, "ValueSegment insufficiently pre-allocated.");
          value_segment->resize(new_size);
        });

        // Make sure the first column's resize actually happens last and does not get reordered.
        std::atomic_thread_fence(std::memory_order_seq_cst);
      }

      if (new_size == target_size) {
        // Allow the chunk to be marked as immutable as it has reached its target size and no incoming Insert operators
        // will try to write to it. Pending Insert operators (including us) will call `try_set_immutable()` to make the
        // chunk immutable once they commit/roll back.
        target_chunk->mark_as_full();
      }

      remaining_rows -= num_rows_for_target_chunk;
    }
  }

  /**
   * 2. Insert the Data into the memory allocated in the first step without holding a lock on the Table.
   */
  auto source_row_id = RowID{ChunkID{0}, ChunkOffset{0}};

  for (const auto& target_chunk_range : _target_chunk_ranges) {
    const auto target_chunk = _target_table->get_chunk(target_chunk_range.chunk_id);

    auto target_chunk_offset = target_chunk_range.begin_chunk_offset;
    auto target_chunk_range_remaining_rows =
        ChunkOffset{target_chunk_range.end_chunk_offset - target_chunk_range.begin_chunk_offset};

    while (target_chunk_range_remaining_rows > 0) {
      const auto source_chunk = left_input_table()->get_chunk(source_row_id.chunk_id);
      const auto source_chunk_remaining_rows = ChunkOffset{source_chunk->size() - source_row_id.chunk_offset};
      const auto num_rows_current_iteration =
          std::min<ChunkOffset>(source_chunk_remaining_rows, target_chunk_range_remaining_rows);

      // Copy from the source into the target Segments.
      const auto column_count = target_chunk->column_count();
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        const auto& source_segment = source_chunk->get_segment(column_id);
        const auto& target_segment = target_chunk->get_segment(column_id);

        resolve_data_type(_target_table->column_data_type(column_id), [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;
          copy_value_range<ColumnDataType>(source_segment, source_row_id.chunk_offset, target_segment,
                                           target_chunk_offset, num_rows_current_iteration);
        });
      }

      if (num_rows_current_iteration == source_chunk_remaining_rows) {
        // Proceed to next source Chunk
        ++source_row_id.chunk_id;
        source_row_id.chunk_offset = 0;
      } else {
        source_row_id.chunk_offset += num_rows_current_iteration;
      }

      target_chunk_offset += num_rows_current_iteration;
      target_chunk_range_remaining_rows -= num_rows_current_iteration;
    }
  }

  return nullptr;
}

void Insert::_on_commit_records(const CommitID cid) {
  for (const auto& target_chunk_range : _target_chunk_ranges) {
    const auto target_chunk = _target_table->get_chunk(target_chunk_range.chunk_id);
    const auto& mvcc_data = target_chunk->mvcc_data();

    for (auto chunk_offset = target_chunk_range.begin_chunk_offset; chunk_offset < target_chunk_range.end_chunk_offset;
         ++chunk_offset) {
      mvcc_data->set_begin_cid(chunk_offset, cid, std::memory_order_relaxed);
      mvcc_data->set_tid(chunk_offset, TransactionID{0}, std::memory_order_relaxed);
    }

    set_atomic_max(mvcc_data->max_begin_cid, cid);

    // This fence ensures that the changes to TID (which are not sequentially consistent) are visible to other threads.
    std::atomic_thread_fence(std::memory_order_release);

    // Deregister the pending Insert and try to mark the chunk as immutable. We might be the last committing Insert
    // operator inserting into a chunk that reached its target size. In this case, the Insert operator that added a new
    // chunk to the table allowed the chunk to be marked, i.e., it set the `reached_target_size` flag. Then,
    // `try_set_immutable()` actually marks the chunk. Otherwise, this is a no-op.
    mvcc_data->deregister_insert();
    target_chunk->try_set_immutable();
  }
}

void Insert::_on_rollback_records() {
  for (const auto& target_chunk_range : _target_chunk_ranges) {
    const auto target_chunk = _target_table->get_chunk(target_chunk_range.chunk_id);
    const auto& mvcc_data = target_chunk->mvcc_data();

    /**
     * !!! Crucial comment, PLEASE READ AND _UNDERSTAND_ before altering any of the following code !!!
     *
     * Set end_cids to 0 (effectively making the rows invisible for everyone) BEFORE setting the begin_cids to 0.
     *
     * Otherwise, another transaction/thread might observe a row with begin_cid == 0, end_cid == MAX_COMMIT_ID and a
     * foreign TID - which is what a visible row that is being deleted by a different transaction looks like. Thus,
     * the other transaction would consider the row (that is in the process of being rolled back and should have never
     * been visible) as visible.
     *
     * We need to set `begin_cid = 0` so that the ChunkCompressionTask can identify "completed" Chunks.
     */

    for (auto chunk_offset = target_chunk_range.begin_chunk_offset; chunk_offset < target_chunk_range.end_chunk_offset;
         ++chunk_offset) {
      // We use a strong sequential memory ordering here to be on the safe side. If rollbacks become a performance
      // bottleneck, the memory order should be revisited.
      mvcc_data->set_end_cid(chunk_offset, UNSET_COMMIT_ID, std::memory_order_seq_cst);
      mvcc_data->set_begin_cid(chunk_offset, UNSET_COMMIT_ID, std::memory_order_seq_cst);
      mvcc_data->set_tid(chunk_offset, TransactionID{0}, std::memory_order_seq_cst);
      // Update chunk statistics.
      target_chunk->increase_invalid_row_count(ChunkOffset{1}, std::memory_order_seq_cst);
    }

    // Deregister the pending Insert and try to mark the chunk as immutable. We might be the last rolling back Insert
    // operator inserting into a chunk that reached its target size. In this case, the Insert operator that added a new
    // chunk to the table allowed the chunk to be marked, i.e., it set the `reached_target_size` flag. Then,
    // `try_set_immutable()` actually marks the chunk. Otherwise, this is a no-op.
    mvcc_data->deregister_insert();
    target_chunk->try_set_immutable();
  }
}

std::shared_ptr<AbstractOperator> Insert::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Insert>(_target_table_name, copied_left_input);
}

void Insert::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace hyrise
