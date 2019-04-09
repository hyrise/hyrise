#include "insert.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "resolve_type.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/value_segment.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace {

using namespace opossum;  // NOLINT

class BaseColumnTypeWrapper : public Noncopyable {
 public:
  virtual ~BaseColumnTypeWrapper() = default;

  virtual void resize_value_segment(std::shared_ptr<BaseSegment> segment, size_t new_size) = 0;
  virtual void copy(const std::shared_ptr<const BaseSegment>& source, ChunkOffset source_start_index,
                    const std::shared_ptr<BaseSegment>& target, ChunkOffset target_start_index, ChunkOffset length) = 0;
};

template <typename T>
class ColumnTypeWrapper : public BaseColumnTypeWrapper {
 public:
  void resize_value_segment(std::shared_ptr<BaseSegment> segment, size_t new_size) override {
    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);
    Assert(value_segment, "Cannot insert into non-ValueColumns");

    value_segment->values().resize(new_size);

    if (value_segment->is_nullable()) {
      value_segment->null_values().resize(new_size);
    }
  }

  void copy(const std::shared_ptr<const BaseSegment>& source_base_segment, ChunkOffset source_start_index,
            const std::shared_ptr<BaseSegment>& target_base_segment, ChunkOffset target_start_index,
            ChunkOffset length) override {
    const auto target_value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(target_base_segment);
    Assert(target_value_segment, "Cannot insert into non-ValueColumns");

    auto& target_values = target_value_segment->values();
    const auto target_is_nullable = target_value_segment->is_nullable();

    /**
     * If the source Segment is a ValueSegment, take a fast path to copy the data.
     * Otherwise, take a (potentially slower) fallback path.
     */
    if (const auto source_value_segment = std::dynamic_pointer_cast<const ValueSegment<T>>(source_base_segment)) {
      std::copy_n(source_value_segment->values().begin() + source_start_index, length,
                  target_values.begin() + target_start_index);

      if (source_value_segment->is_nullable()) {
        const auto nulls_begin_iter = source_value_segment->null_values().begin() + source_start_index;
        const auto nulls_end_iter = nulls_begin_iter + length;

        Assert(
            target_is_nullable || std::none_of(nulls_begin_iter, nulls_end_iter, [](const auto& null) { return null; }),
            "Trying to insert NULL into non-NULL segment");
        std::copy(nulls_begin_iter, nulls_end_iter, target_value_segment->null_values().begin() + target_start_index);
      }
    } else {
      segment_with_iterators<T>(*source_base_segment, [&](auto source_begin, const auto source_end) {
        auto source_iter = source_begin + source_start_index;
        auto target_iter = target_values.begin() + target_start_index;

        // Copy values and null values
        for (auto i = 0u; i < length; i++) {
          *target_iter = source_iter->value();

          if (target_is_nullable) {
            target_value_segment->null_values()[target_start_index + i] = source_iter->is_null();
          } else {
            Assert(!source_iter->is_null(), "Cannot insert NULL into NOT NULL target");
          }

          ++source_iter;
          ++target_iter;
        }
      });
    }
  }
};

}  // namespace

namespace opossum {

Insert::Insert(const std::string& target_table_name, const std::shared_ptr<const AbstractOperator>& values_to_insert)
    : AbstractReadWriteOperator(OperatorType::Insert, values_to_insert), _target_table_name(target_table_name) {}

const std::string Insert::name() const { return "Insert"; }

std::string Insert::target_table_name() const { return _target_table_name; }

std::shared_ptr<const Table> Insert::_on_execute(std::shared_ptr<TransactionContext> context) {
  context->register_read_write_operator(std::static_pointer_cast<AbstractReadWriteOperator>(shared_from_this()));

  _target_table = StorageManager::get().get_table(_target_table_name);

  Assert(_target_table->max_chunk_size() > 0, "Expected max chunk size of target table to be greater than zero");

  // Create TypedSegmentProcessors
  auto typed_segment_processors = std::vector<std::unique_ptr<BaseColumnTypeWrapper>>();
  for (const auto& column_type : _target_table->column_data_types()) {
    typed_segment_processors.emplace_back(
        make_unique_by_data_type<BaseColumnTypeWrapper, ColumnTypeWrapper>(column_type));
  }

  /**
   * 1. Allocate the required rows before actually writing to them. Do so while locking the table to prevent multiple
   *    threads modifying the table's size simultaneously.
   *    Since allocation is expected to be faster than writing to the memory, this will minimize the time that the 
   *    Table's append_mutex is locked.
   */
  {
    const auto append_lock = _target_table->acquire_append_mutex();

    auto remaining_rows = input_table_left()->row_count();

    if (_target_table->chunk_count() == 0) {
      _target_table->append_mutable_chunk();
    }

    while (remaining_rows > 0) {
      auto target_chunk_id = ChunkID{_target_table->chunk_count() - 1};
      auto target_chunk = _target_table->get_chunk(target_chunk_id);

      // If the last Chunk of the target Table is either immutable or full, append a new mutable Chunk
      if (!target_chunk->is_mutable() || target_chunk->size() == _target_table->max_chunk_size()) {
        _target_table->append_mutable_chunk();
        ++target_chunk_id;
        target_chunk = _target_table->get_chunk(target_chunk_id);
      }

      const auto target_chunk_num_inserted_rows =
          std::min<size_t>(_target_table->max_chunk_size() - target_chunk->size(), remaining_rows);

      _target_chunk_ranges.emplace_back(
          ChunkRange{target_chunk_id, target_chunk->size(),
                     static_cast<ChunkOffset>(target_chunk->size() + target_chunk_num_inserted_rows)});

      // Grow MVCC columns.
      target_chunk->get_scoped_mvcc_data_lock()->grow_by(target_chunk_num_inserted_rows, MvccData::MAX_COMMIT_ID);

      // Grow data Segments.
      auto old_size = target_chunk->size();
      for (ColumnID column_id{0}; column_id < target_chunk->column_count(); ++column_id) {
        typed_segment_processors[column_id]->resize_value_segment(target_chunk->get_segment(column_id),
                                                                  old_size + target_chunk_num_inserted_rows);
      }

      remaining_rows -= target_chunk_num_inserted_rows;
    }
  }

  // TODO(all): make compress chunk thread-safe; if it gets called here by another thread, things will likely break.

  /**
   * 2. Insert the Data into the memory allocated in the first step. Write the transaction_context's transaction_id into
   *    all allocated rows.
   */
  auto input_row_id = RowID{ChunkID{0}, ChunkOffset{0}};

  for (const auto& target_chunk_range : _target_chunk_ranges) {
    const auto target_chunk = _target_table->get_chunk(target_chunk_range.chunk_id);

    auto target_chunk_offset = target_chunk_range.begin_chunk_offset;
    auto target_chunk_range_remaining_rows =
        target_chunk_range.end_chunk_offset - target_chunk_range.begin_chunk_offset;

    while (target_chunk_range_remaining_rows > 0) {
      const auto input_chunk = input_table_left()->get_chunk(input_row_id.chunk_id);
      const auto input_chunk_remaining_rows = input_chunk->size() - input_row_id.chunk_offset;
      const auto input_chunk_num_rows = std::min(input_chunk_remaining_rows, target_chunk_range_remaining_rows);

      // Copy from the input into the target Segments
      for (ColumnID column_id{0}; column_id < target_chunk->column_count(); ++column_id) {
        const auto input_segment = input_chunk->get_segment(column_id);
        typed_segment_processors[column_id]->copy(input_segment, input_row_id.chunk_offset,
                                                  target_chunk->get_segment(column_id), target_chunk_offset,
                                                  input_chunk_num_rows);
      }

      if (input_chunk_num_rows == input_chunk_remaining_rows) {
        // Proceed to next input Chunk
        ++input_row_id.chunk_id;
        input_row_id.chunk_offset = 0;
      } else {
        input_row_id.chunk_offset += input_chunk_num_rows;
      }

      target_chunk_offset += input_chunk_num_rows;
      target_chunk_range_remaining_rows -= input_chunk_num_rows;
    }

    // Write the transaction_context's transaction_id into all new rows
    for (auto chunk_offset = target_chunk_range.begin_chunk_offset; chunk_offset < target_chunk_range.end_chunk_offset;
         ++chunk_offset) {
      // we do not need to check whether other operators have locked the rows, we have just created them
      // and they are not visible for other operators.
      // the transaction IDs are set here and not during the resize, because
      // tbb::concurrent_vector::grow_to_at_least(n, t)" does not work with atomics, since their copy constructor is
      // deleted.
      target_chunk->get_scoped_mvcc_data_lock()->tids[chunk_offset] = context->transaction_id();
    }
  }

  return nullptr;
}

void Insert::_on_commit_records(const CommitID cid) {
  for (const auto& target_chunk_range : _target_chunk_ranges) {
    const auto target_chunk = _target_table->get_chunk(target_chunk_range.chunk_id);
    auto mvcc_data = target_chunk->get_scoped_mvcc_data_lock();

    for (auto chunk_offset = target_chunk_range.begin_chunk_offset; chunk_offset < target_chunk_range.end_chunk_offset;
         ++chunk_offset) {
      mvcc_data->begin_cids[chunk_offset] = cid;
      mvcc_data->tids[chunk_offset] = 0u;
    }
  }
}

void Insert::_on_rollback_records() {
  for (const auto& target_chunk_range : _target_chunk_ranges) {
    const auto target_chunk = _target_table->get_chunk(target_chunk_range.chunk_id);
    auto mvcc_data = target_chunk->get_scoped_mvcc_data_lock();

    for (auto chunk_offset = target_chunk_range.begin_chunk_offset; chunk_offset < target_chunk_range.end_chunk_offset;
         ++chunk_offset) {
      // Set the begin and end cids to 0 (effectively making it invisible for everyone).
      // Make sure that the end is written before the begin.
      mvcc_data->end_cids[chunk_offset] = 0u;
      std::atomic_thread_fence(std::memory_order_release);
      mvcc_data->begin_cids[chunk_offset] = 0u;
      mvcc_data->tids[chunk_offset] = 0u;
    }
  }
}

std::shared_ptr<AbstractOperator> Insert::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Insert>(_target_table_name, copied_input_left);
}

void Insert::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
