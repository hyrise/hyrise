#include "insert.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "storage/base_dictionary_column.hpp"
#include "storage/storage_manager.hpp"
#include "storage/value_column.hpp"
#include "utils/assert.hpp"

#include "resolve_type.hpp"
#include "type_cast.hpp"

namespace opossum {

// We need these classes to perform the dynamic cast into a templated ValueColumn
class AbstractTypedColumnProcessor {
 public:
  virtual void resize_vector(std::shared_ptr<BaseColumn> column, size_t new_size) = 0;
  virtual void copy_data(std::shared_ptr<BaseColumn> source, size_t source_start_index,
                         std::shared_ptr<BaseColumn> target, size_t target_start_index, size_t length) = 0;
};

template <typename T>
class TypedColumnProcessor : public AbstractTypedColumnProcessor {
 public:
  void resize_vector(std::shared_ptr<BaseColumn> column, size_t new_size) override {
    auto casted_col = std::dynamic_pointer_cast<ValueColumn<T>>(column);
    DebugAssert(static_cast<bool>(casted_col), "Type mismatch");
    auto& vect = casted_col->values();

    vect.resize(new_size);
  }

  // this copies
  void copy_data(std::shared_ptr<BaseColumn> source, size_t source_start_index, std::shared_ptr<BaseColumn> target,
                 size_t target_start_index, size_t length) override {
    auto casted_target = std::dynamic_pointer_cast<ValueColumn<T>>(target);
    DebugAssert(static_cast<bool>(casted_target), "Type mismatch");
    auto& vect = casted_target->values();

    if (auto casted_source = std::dynamic_pointer_cast<ValueColumn<T>>(source)) {
      std::copy_n(casted_source->values().begin() + source_start_index, length, vect.begin() + target_start_index);
      // } else if(auto casted_source = std::dynamic_pointer_cast<ReferenceColumn>(source)){
      // since we have no guarantee that a referenceColumn references only a single other column,
      // this would require us to find out the referenced column's type for each single row.
      // instead, we just use the slow path below.
    } else {
      for (auto i = 0u; i < length; i++) {
        vect[target_start_index + i] = type_cast<T>((*source)[source_start_index + i]);
      }
    }
  }
};

Insert::Insert(const std::string& target_table_name, const std::shared_ptr<AbstractOperator>& values_to_insert)
    : AbstractReadWriteOperator(values_to_insert), _target_table_name(target_table_name) {}

const std::string Insert::name() const { return "Insert"; }

uint8_t Insert::num_in_tables() const { return 1; }

std::shared_ptr<const Table> Insert::_on_execute(std::shared_ptr<TransactionContext> context) {
  context->register_rw_operator(shared_from_this());

  _target_table = StorageManager::get().get_table(_target_table_name);

  // These TypedColumnProcessors kind of retrieve the template parameter of the columns.
  auto typed_column_processors = std::vector<std::unique_ptr<AbstractTypedColumnProcessor>>();
  for (ColumnID column_id{0}; column_id < _target_table->get_chunk(ChunkID{0}).col_count(); ++column_id) {
    typed_column_processors.emplace_back(make_unique_by_column_type<AbstractTypedColumnProcessor, TypedColumnProcessor>(
        _target_table->column_type(column_id)));
  }

  auto total_rows_to_insert = 0u;

  for (auto i = ChunkID{0}; i < _input_table_left()->chunk_count(); i++) {
    const auto& chunk = _input_table_left()->get_chunk(i);
    total_rows_to_insert += chunk.size();
  }

  // First, allocate space for all the rows to insert. Do so while locking the table
  // to prevent multiple threads modifying the table's size simultaneously.
  auto start_index = 0u;
  auto start_chunk_id = ChunkID{0};
  auto total_chunks_inserted = 0u;
  {
    auto scoped_lock = _target_table->acquire_append_mutex();

    start_chunk_id = _target_table->chunk_count() - 1;
    auto& last_chunk = _target_table->get_chunk(start_chunk_id);
    start_index = last_chunk.size();

    // If last chunk is compressed, add a new uncompressed chunk
    if (std::dynamic_pointer_cast<BaseDictionaryColumn>(last_chunk.get_column(ColumnID{0})) != nullptr) {
      _target_table->create_new_chunk();
      total_chunks_inserted++;
    }

    auto remaining_rows = total_rows_to_insert;
    while (remaining_rows > 0) {
      auto& current_chunk = _target_table->get_chunk(static_cast<ChunkID>(_target_table->chunk_count() - 1));
      auto rows_to_insert_this_loop = std::min(_target_table->chunk_size() - current_chunk.size(), remaining_rows);

      // Resize MVCC vectors.
      current_chunk.grow_mvcc_column_size_by(rows_to_insert_this_loop, Chunk::MAX_COMMIT_ID);

      // Resize current chunk to full size.
      auto old_size = current_chunk.size();
      for (ColumnID i{0}; i < current_chunk.col_count(); ++i) {
        typed_column_processors[i]->resize_vector(current_chunk.get_column(i), old_size + rows_to_insert_this_loop);
      }

      remaining_rows -= rows_to_insert_this_loop;

      // Create new chunk if necessary.
      if (remaining_rows > 0) {
        _target_table->create_new_chunk();
        total_chunks_inserted++;
      }
    }
  }
  // TODO(all): make compress chunk thread-safe; if it gets called here by another thread, things will likely break.

  // Then, actually insert the data.
  auto input_offset = 0u;
  auto source_chunk_id = ChunkID{0};
  auto source_chunk_start_index = 0u;

  for (auto target_chunk_id = start_chunk_id; target_chunk_id <= start_chunk_id + total_chunks_inserted;
       target_chunk_id++) {
    const auto curr_num_rows_to_insert =
        std::min(_target_table->get_chunk(target_chunk_id).size() - start_index, total_rows_to_insert - input_offset);

    auto& target_chunk = _target_table->get_chunk(target_chunk_id);

    auto target_start_index = start_index;
    auto n = curr_num_rows_to_insert;

    // while target chunk is not full
    while (target_start_index != target_chunk.size()) {
      const auto& source_chunk = _input_table_left()->get_chunk(source_chunk_id);
      auto num_to_insert = std::min(source_chunk.size() - source_chunk_start_index, n);
      for (ColumnID i{0}; i < target_chunk.col_count(); ++i) {
        auto source_column = source_chunk.get_column(i);
        typed_column_processors[i]->copy_data(source_column, source_chunk_start_index, target_chunk.get_column(i),
                                              target_start_index, num_to_insert);
      }
      n -= num_to_insert;
      target_start_index += num_to_insert;

      source_chunk_start_index += num_to_insert;

      bool source_chunk_depleted = source_chunk_start_index == source_chunk.size();
      if (source_chunk_depleted) {
        source_chunk_id++;
        source_chunk_start_index = 0u;
      }
    }

    for (auto i = start_index; i < start_index + curr_num_rows_to_insert; i++) {
      // we do not need to check whether other operators have locked the rows, we have just created them
      // and they are not visible for other operators.
      // the transaction IDs are set here and not during the resize, because
      // tbb::concurrent_vector::grow_to_at_least(n, t)" does not work with atomics, since their copy constructor is
      // deleted.
      target_chunk.mvcc_columns()->tids[i] = context->transaction_id();
      _inserted_rows.emplace_back(_target_table->calculate_row_id(target_chunk_id, i));
    }

    input_offset += curr_num_rows_to_insert;
    start_index = 0u;
  }

  return nullptr;
}

void Insert::commit_records(const CommitID cid) {
  for (auto row_id : _inserted_rows) {
    auto& chunk = _target_table->get_chunk(row_id.chunk_id);

    auto mvcc_columns = chunk.mvcc_columns();
    mvcc_columns->begin_cids[row_id.chunk_offset] = cid;
    mvcc_columns->tids[row_id.chunk_offset] = 0u;
  }
}

void Insert::rollback_records() {
  for (auto row_id : _inserted_rows) {
    auto& chunk = _target_table->get_chunk(row_id.chunk_id);
    chunk.mvcc_columns()->tids[row_id.chunk_offset] = 0u;
  }
}

}  // namespace opossum
