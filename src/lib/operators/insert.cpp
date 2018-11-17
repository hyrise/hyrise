#include "insert.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "resolve_type.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/storage_manager.hpp"
#include "storage/value_segment.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

// We need these classes to perform the dynamic cast into a templated ValueSegment
class AbstractTypedSegmentProcessor : public Noncopyable {
 public:
  AbstractTypedSegmentProcessor() = default;
  AbstractTypedSegmentProcessor(const AbstractTypedSegmentProcessor&) = delete;
  AbstractTypedSegmentProcessor& operator=(const AbstractTypedSegmentProcessor&) = delete;
  AbstractTypedSegmentProcessor(AbstractTypedSegmentProcessor&&) = default;
  AbstractTypedSegmentProcessor& operator=(AbstractTypedSegmentProcessor&&) = default;
  virtual ~AbstractTypedSegmentProcessor() = default;
  virtual void resize_vector(std::shared_ptr<BaseSegment> segment, size_t new_size) = 0;
  virtual void copy_data(std::shared_ptr<const BaseSegment> source, size_t source_start_index,
                         std::shared_ptr<BaseSegment> target, size_t target_start_index, size_t length) = 0;
};

template <typename T>
class TypedSegmentProcessor : public AbstractTypedSegmentProcessor {
 public:
  void resize_vector(std::shared_ptr<BaseSegment> segment, size_t new_size) override {
    auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);
    DebugAssert(value_segment, "Cannot insert into non-ValueColumns");
    auto& values = value_segment->values();

    values.resize(new_size);

    if (value_segment->is_nullable()) {
      value_segment->null_values().resize(new_size);
    }
  }

  // this copies
  void copy_data(std::shared_ptr<const BaseSegment> source, size_t source_start_index,
                 std::shared_ptr<BaseSegment> target, size_t target_start_index, size_t length) override {
    auto casted_target = std::dynamic_pointer_cast<ValueSegment<T>>(target);
    DebugAssert(casted_target, "Cannot insert into non-ValueColumns");
    auto& values = casted_target->values();

    auto target_is_nullable = casted_target->is_nullable();

    if (auto casted_source = std::dynamic_pointer_cast<const ValueSegment<T>>(source)) {
      std::copy_n(casted_source->values().begin() + source_start_index, length, values.begin() + target_start_index);

      if (casted_source->is_nullable()) {
        const auto nulls_begin_iter = casted_source->null_values().begin() + source_start_index;
        const auto nulls_end_iter = nulls_begin_iter + length;

        Assert(
            target_is_nullable || std::none_of(nulls_begin_iter, nulls_end_iter, [](const auto& null) { return null; }),
            "Trying to insert NULL into non-NULL segment");
        std::copy(nulls_begin_iter, nulls_end_iter, casted_target->null_values().begin() + target_start_index);
      }
    } else if (auto casted_dummy_source = std::dynamic_pointer_cast<const ValueSegment<int32_t>>(source)) {
      // We use the segment type of the Dummy table used to insert a single null value.
      // A few asserts are needed to guarantee correct behaviour.

      // You may also end up here if the data types of the input and the target column mismatch. Usually, this gets
      // caught way earlier, but if you build your own tests, this might happen.
      Assert(length == 1, "Cannot insert multiple unknown null values at once.");
      Assert(casted_dummy_source->size() == 1, "Source segment is of wrong type.");
      Assert(casted_dummy_source->null_values().front() == true, "Only value in dummy table must be NULL!");
      Assert(target_is_nullable, "Cannot insert NULL into NOT NULL target.");

      // Ignore source value and only set null to true
      casted_target->null_values()[target_start_index] = true;
    } else {
      // } else if(auto casted_source = std::dynamic_pointer_cast<ReferenceSegment>(source)){
      // since we have no guarantee that a ReferenceSegment references only a single other segment,
      // this would require us to find out the referenced segment's type for each single row.
      // instead, we just use the slow path below.
      for (auto i = 0u; i < length; i++) {
        auto ref_value = (*source)[source_start_index + i];
        if (variant_is_null(ref_value)) {
          Assert(target_is_nullable, "Cannot insert NULL into NOT NULL target");
          values[target_start_index + i] = T{};
          casted_target->null_values()[target_start_index + i] = true;
        } else {
          values[target_start_index + i] = type_cast_variant<T>(ref_value);
        }
      }
    }
  }
};

Insert::Insert(const std::string& target_table_name, const std::shared_ptr<AbstractOperator>& values_to_insert)
    : AbstractReadWriteOperator(OperatorType::Insert, values_to_insert), _target_table_name(target_table_name) {}

const std::string Insert::name() const { return "Insert"; }

std::shared_ptr<const Table> Insert::_on_execute(std::shared_ptr<TransactionContext> context) {
  context->register_read_write_operator(std::static_pointer_cast<AbstractReadWriteOperator>(shared_from_this()));

  _target_table = StorageManager::get().get_table(_target_table_name);

  // These TypedSegmentProcessors kind of retrieve the template parameter of the segments.
  auto typed_segment_processors = std::vector<std::unique_ptr<AbstractTypedSegmentProcessor>>();
  for (const auto& column_type : _target_table->column_data_types()) {
    typed_segment_processors.emplace_back(
        make_unique_by_data_type<AbstractTypedSegmentProcessor, TypedSegmentProcessor>(column_type));
  }

  auto total_rows_to_insert = static_cast<uint32_t>(input_table_left()->row_count());

  // First, allocate space for all the rows to insert. Do so while locking the table to prevent multiple threads
  // modifying the table's size simultaneously.
  auto start_index = 0u;
  auto start_chunk_id = ChunkID{0};
  auto end_chunk_id = 0u;
  {
    auto scoped_lock = _target_table->acquire_append_mutex();

    if (_target_table->chunk_count() == 0) {
      _target_table->append_mutable_chunk();
    }

    start_chunk_id = _target_table->chunk_count() - 1;
    end_chunk_id = start_chunk_id + 1;
    auto last_chunk = _target_table->get_chunk(start_chunk_id);
    start_index = last_chunk->size();

    // If last chunk is compressed, add a new uncompressed chunk
    if (!last_chunk->is_mutable()) {
      _target_table->append_mutable_chunk();
      end_chunk_id++;
    }

    auto remaining_rows = total_rows_to_insert;
    while (remaining_rows > 0) {
      auto current_chunk = _target_table->get_chunk(static_cast<ChunkID>(_target_table->chunk_count() - 1));
      auto rows_to_insert_this_loop = std::min(_target_table->max_chunk_size() - current_chunk->size(), remaining_rows);

      // Resize MVCC vectors.
      current_chunk->get_scoped_mvcc_data_lock()->grow_by(rows_to_insert_this_loop, MvccData::MAX_COMMIT_ID);

      // Resize current chunk to full size.
      auto old_size = current_chunk->size();
      for (ColumnID column_id{0}; column_id < current_chunk->column_count(); ++column_id) {
        typed_segment_processors[column_id]->resize_vector(current_chunk->get_segment(column_id),
                                                           old_size + rows_to_insert_this_loop);
      }

      remaining_rows -= rows_to_insert_this_loop;

      // Create new chunk if necessary.
      if (remaining_rows > 0) {
        _target_table->append_mutable_chunk();
        end_chunk_id++;
      }
    }
  }
  // TODO(all): make compress chunk thread-safe; if it gets called here by another thread, things will likely break.

  // Then, actually insert the data.
  auto input_offset = 0u;
  auto source_chunk_id = ChunkID{0};
  auto source_chunk_start_index = 0u;

  for (auto target_chunk_id = start_chunk_id; target_chunk_id < end_chunk_id; target_chunk_id++) {
    auto target_chunk = _target_table->get_chunk(target_chunk_id);

    const auto current_num_rows_to_insert =
        std::min(target_chunk->size() - start_index, total_rows_to_insert - input_offset);

    auto target_start_index = start_index;
    auto still_to_insert = current_num_rows_to_insert;

    // while target chunk is not full
    while (target_start_index != target_chunk->size()) {
      const auto source_chunk = input_table_left()->get_chunk(source_chunk_id);
      auto num_to_insert = std::min(source_chunk->size() - source_chunk_start_index, still_to_insert);
      for (ColumnID column_id{0}; column_id < target_chunk->column_count(); ++column_id) {
        const auto& source_segment = source_chunk->get_segment(column_id);
        typed_segment_processors[column_id]->copy_data(source_segment, source_chunk_start_index,
                                                       target_chunk->get_segment(column_id), target_start_index,
                                                       num_to_insert);
      }
      still_to_insert -= num_to_insert;
      target_start_index += num_to_insert;
      source_chunk_start_index += num_to_insert;

      bool source_chunk_depleted = source_chunk_start_index == source_chunk->size();
      if (source_chunk_depleted) {
        source_chunk_id++;
        source_chunk_start_index = 0u;
      }
    }

    for (auto i = start_index; i < start_index + current_num_rows_to_insert; i++) {
      // we do not need to check whether other operators have locked the rows, we have just created them
      // and they are not visible for other operators.
      // the transaction IDs are set here and not during the resize, because
      // tbb::concurrent_vector::grow_to_at_least(n, t)" does not work with atomics, since their copy constructor is
      // deleted.
      target_chunk->get_scoped_mvcc_data_lock()->tids[i] = context->transaction_id();
      _inserted_rows.emplace_back(RowID{target_chunk_id, i});
    }

    input_offset += current_num_rows_to_insert;
    start_index = 0u;
  }

  return nullptr;
}

void Insert::_on_commit_records(const CommitID cid) {
  for (auto row_id : _inserted_rows) {
    auto chunk = _target_table->get_chunk(row_id.chunk_id);

    auto mvcc_data = chunk->get_scoped_mvcc_data_lock();
    mvcc_data->begin_cids[row_id.chunk_offset] = cid;
    mvcc_data->tids[row_id.chunk_offset] = 0u;
  }
}

void Insert::_on_rollback_records() {
  for (auto row_id : _inserted_rows) {
    auto chunk = _target_table->get_chunk(row_id.chunk_id);
    // We set the begin and end cids to 0 (effectively making it invisible for everyone) so that the ChunkCompression
    // does not think that this row is still incomplete. We need to make sure that the end is written before the begin.
    chunk->get_scoped_mvcc_data_lock()->end_cids[row_id.chunk_offset] = 0u;
    std::atomic_thread_fence(std::memory_order_release);
    chunk->get_scoped_mvcc_data_lock()->begin_cids[row_id.chunk_offset] = 0u;

    chunk->get_scoped_mvcc_data_lock()->tids[row_id.chunk_offset] = 0u;
  }
}

std::shared_ptr<AbstractOperator> Insert::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Insert>(_target_table_name, copied_input_left);
}

void Insert::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
