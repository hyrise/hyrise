#include "partitioning.hpp"

#include "insert.hpp"
#include "storage/partitioning/hash_partition_schema.hpp"
#include "storage/partitioning/null_partition_schema.hpp"
#include "storage/partitioning/range_partition_schema.hpp"
#include "storage/partitioning/round_robin_partition_schema.hpp"
#include "storage/storage_manager.hpp"
#include "table_wrapper.hpp"

namespace opossum {

// We need these classes to perform the dynamic cast into a templated ValueColumn
class AbstractTypedColumnProcessor {
 public:
  virtual void resize_vector(std::shared_ptr<BaseColumn> column, size_t new_size) = 0;
  virtual void copy_data(std::shared_ptr<const BaseColumn> source, size_t source_start_index,
                         std::shared_ptr<BaseColumn> target, size_t target_start_index,
                         std::vector<size_t> rows_to_copy) = 0;
};

template <typename T>
class TypedColumnProcessor : public AbstractTypedColumnProcessor {
 public:
  void resize_vector(std::shared_ptr<BaseColumn> column, size_t new_size) override {
    auto val_column = std::dynamic_pointer_cast<ValueColumn<T>>(column);
    DebugAssert(static_cast<bool>(val_column), "Type mismatch");
    auto& values = val_column->values();

    values.resize(new_size);

    if (val_column->is_nullable()) {
      val_column->null_values().resize(new_size);
    }
  }

  void copy_data(std::shared_ptr<const BaseColumn> source, size_t source_start_index,
                 std::shared_ptr<BaseColumn> target, size_t target_start_index,
                 std::vector<size_t> rows_to_copy) override {
    auto casted_target = std::dynamic_pointer_cast<ValueColumn<T>>(target);
    DebugAssert(static_cast<bool>(casted_target), "Type mismatch");
    auto& values = casted_target->values();

    auto target_is_nullable = casted_target->is_nullable();

    if (auto casted_source = std::dynamic_pointer_cast<const ValueColumn<T>>(source)) {
      size_t target_index = target_start_index;
      for (size_t row_to_copy : rows_to_copy) {
        if (row_to_copy >= source_start_index) {
          std::copy_n(casted_source->values().begin() + row_to_copy, 1, values.begin() + target_index);
          target_index++;
        }
      }

      if (casted_source->is_nullable()) {
        // Values to insert contain null, copy them
        if (target_is_nullable) {
          target_index = target_start_index;
          for (size_t row_to_copy : rows_to_copy) {
            if (row_to_copy >= source_start_index) {
              std::copy_n(casted_source->null_values().begin() + row_to_copy, 1,
                          casted_target->null_values().begin() + target_index);
              target_index++;
            }
          }
        } else {
          for (const auto null_value : casted_source->null_values()) {
            Assert(!null_value, "Trying to insert NULL into non-NULL column");
          }
        }
      }
    } else if (auto casted_dummy_source = std::dynamic_pointer_cast<const ValueColumn<int32_t>>(source)) {
      // We use the column type of the Dummy table used to insert a single null value.
      // A few asserts are needed to guarantee correct behaviour.
      Assert(rows_to_copy.size() == 1, "Cannot insert multiple unknown null values at once.");
      Assert(casted_dummy_source->size() == 1, "Source column is of wrong type.");
      Assert(casted_dummy_source->null_values().front() == true, "Only value in dummy table must be NULL!");
      Assert(target_is_nullable, "Cannot insert NULL into NOT NULL target.");

      // Ignore source value and only set null to true
      casted_target->null_values()[target_start_index] = true;
    } else {
      // } else if(auto casted_source = std::dynamic_pointer_cast<ReferenceColumn>(source)){
      // since we have no guarantee that a referenceColumn references only a single other column,
      // this would require us to find out the referenced column's type for each single row.
      // instead, we just use the slow path below.

      size_t target_index = target_start_index;
      for (size_t row_to_copy : rows_to_copy) {
        if (row_to_copy >= source_start_index) {
          auto ref_value = (*source)[row_to_copy];
          if (variant_is_null(ref_value)) {
            Assert(target_is_nullable, "Cannot insert NULL into NOT NULL target");
            values[target_index] = T{};
            casted_target->null_values()[target_index] = true;
          } else {
            values[target_index] = type_cast<T>(ref_value);
          }
          target_index++;
        }
      }
    }
  }
};

Partitioning::Partitioning(const std::string& table_to_partition_name,
                           std::shared_ptr<AbstractPartitionSchema> target_partition_schema)
    : AbstractReadWriteOperator(),
      _table_to_partition_name{table_to_partition_name},
      _target_partition_schema{target_partition_schema} {}

const std::string Partitioning::name() const { return "Partitioning"; }

std::shared_ptr<const Table> Partitioning::_on_execute(std::shared_ptr<TransactionContext> context) {
  const auto table_to_partition = _get_table_to_be_partitioned();
  _lock_table(table_to_partition);
  auto partitioned_table = _create_partitioned_table_copy(table_to_partition);
  _copy_table_content(table_to_partition, partitioned_table);
  _replace_table(partitioned_table);

  return nullptr;
}

std::shared_ptr<Table> Partitioning::_get_table_to_be_partitioned() {
  return StorageManager::get().get_table(_table_to_partition_name);
}

std::unique_lock<std::mutex> Partitioning::_lock_table(std::shared_ptr<Table> table_to_lock) {
  return table_to_lock->acquire_append_mutex();
}

std::shared_ptr<Table> Partitioning::_create_partitioned_table_copy(std::shared_ptr<Table> table_to_be_partitioned) {
  auto partitioned_table =
      Table::create_with_layout_from(table_to_be_partitioned, table_to_be_partitioned->max_chunk_size());
  partitioned_table->apply_partitioning(_target_partition_schema);
  return partitioned_table;
}

void Partitioning::_copy_table_content(std::shared_ptr<Table> source, std::shared_ptr<Table> target) {
  // partitioning
  std::map<RowID, PartitionID> target_partition_mapping =
      _map_content_to_add_to_partitions(source, _target_partition_schema);
  std::map<PartitionID, uint32_t> rows_to_add_to_partition = _count_rows_for_partitions(target_partition_mapping);

  // These TypedColumnProcessors kind of retrieve the template parameter of the columns.
  auto typed_column_processors = std::vector<std::unique_ptr<AbstractTypedColumnProcessor>>();
  for (const auto& column_type : target->column_types()) {
    typed_column_processors.emplace_back(
        make_unique_by_data_type<AbstractTypedColumnProcessor, TypedColumnProcessor>(column_type));
  }

  for (std::map<PartitionID, uint32_t>::iterator iter = rows_to_add_to_partition.begin();
       iter != rows_to_add_to_partition.end(); ++iter) {
    PartitionID partitionID = iter->first;
    uint32_t total_rows_to_insert = iter->second;

    auto partition = target->get_partition_schema()->get_partition(partitionID);

    // First, allocate space for all the rows to insert. Do so while locking the table to prevent multiple threads
    // modifying the table's size simultaneously.
    auto start_index = 0u;
    auto start_chunk_id = ChunkID{0};
    std::vector<ChunkID> chunks_to_add_in_partition;
    auto total_chunks_inserted = 0u;
    {
      auto scoped_lock = target->acquire_append_mutex();

      start_chunk_id = partition->last_chunk()->id();
      chunks_to_add_in_partition.emplace_back(start_chunk_id);
      auto last_chunk = target->get_chunk(start_chunk_id);
      start_index = last_chunk->size();

      auto partition = target->get_partition_schema()->get_partition(partitionID);

      auto remaining_rows = total_rows_to_insert;
      while (remaining_rows > 0) {
        ChunkID current_chunk_id = partition->last_chunk()->id();
        auto current_chunk = target->get_mutable_chunk(current_chunk_id);
        auto rows_to_insert_this_loop = std::min(target->max_chunk_size() - current_chunk->size(), remaining_rows);

        // Resize MVCC vectors.
        current_chunk->grow_mvcc_column_size_by(rows_to_insert_this_loop, Chunk::MAX_COMMIT_ID);

        // Resize current chunk to full size.
        auto old_size = current_chunk->size();
        for (ColumnID column_id{0}; column_id < current_chunk->column_count(); ++column_id) {
          typed_column_processors[column_id]->resize_vector(current_chunk->get_mutable_column(column_id),
                                                            old_size + rows_to_insert_this_loop);
        }

        remaining_rows -= rows_to_insert_this_loop;

        // Create new chunk if necessary.
        if (remaining_rows > 0) {
          target->create_new_chunk(partitionID);
          total_chunks_inserted++;

          ChunkID chunk_id_to_add = partition->last_chunk()->id();
          chunks_to_add_in_partition.emplace_back(chunk_id_to_add);
        }
      }
    }
    // TODO(all): make compress chunk thread-safe; if it gets called here by another thread, things will likely break.

    // Then, actually insert the data.
    auto input_offset = 0u;
    auto source_chunk_id = ChunkID{0};
    auto source_chunk_start_index = 0u;

    std::vector<RowID> rows_to_insert_into_partition;
    for (std::map<RowID, PartitionID>::iterator iter = target_partition_mapping.begin();
         iter != target_partition_mapping.end(); ++iter) {
      if (iter->second == partitionID) {
        rows_to_insert_into_partition.emplace_back(iter->first);
      }
    }

    for (auto target_chunk_id : chunks_to_add_in_partition) {
      // for (auto target_chunk_id = start_chunk_id; target_chunk_id <= start_chunk_id + total_chunks_inserted;
      // target_chunk_id++) {
      auto target_chunk = target->get_mutable_chunk(target_chunk_id);

      const auto current_num_rows_to_insert =
          std::min(target_chunk->size() - start_index, total_rows_to_insert - input_offset);

      auto target_start_index = start_index;
      auto still_to_insert = current_num_rows_to_insert;

      // while target chunk is not full
      while (target_start_index != target_chunk->size()) {
        const auto source_chunk = source->get_chunk(source_chunk_id);
        auto num_to_insert = std::min(source_chunk->size() - source_chunk_start_index, still_to_insert);

        std::vector<size_t> rows_to_copy;
        size_t last_touched_index_source_chunk = 0;
        if (source_chunk->column_count() >= 1) {
          // doing this for one column only is enough because the rows to copy are the same in all columns
          for (size_t row = source_chunk_start_index; row < source_chunk->get_column(ColumnID{0})->size(); ++row) {
            if (target_partition_mapping[{source_chunk_id, static_cast<ChunkOffset>(row)}] == partitionID &&
                rows_to_copy.size() <= num_to_insert) {
              rows_to_copy.push_back(row);
              last_touched_index_source_chunk = row;
            }
          }
        }

        for (ColumnID column_id{0}; column_id < target_chunk->column_count(); ++column_id) {
          const auto& source_column = source_chunk->get_column(column_id);
          typed_column_processors[column_id]->copy_data(source_column, source_chunk_start_index,
                                                        target_chunk->get_mutable_column(column_id), target_start_index,
                                                        rows_to_copy);
        }

        for (auto i = target_start_index, j = 0u; i < target_start_index + num_to_insert && j < rows_to_copy.size();
             i++, j++) {
          target_chunk->mvcc_columns()->tids[i] =
              source->get_chunk(source_chunk_id)->mvcc_columns()->tids[rows_to_copy[j]];
          target_chunk->mvcc_columns()->begin_cids[i] =
              source->get_chunk(source_chunk_id)->mvcc_columns()->begin_cids[rows_to_copy[j]];
          target_chunk->mvcc_columns()->end_cids[i] =
              source->get_chunk(source_chunk_id)->mvcc_columns()->end_cids[rows_to_copy[j]];
        }

        still_to_insert -= num_to_insert;
        target_start_index += num_to_insert;
        source_chunk_start_index = last_touched_index_source_chunk + 1;

        bool source_chunk_depleted = source_chunk_start_index == source_chunk->size();
        if (source_chunk_depleted) {
          source_chunk_id++;
          source_chunk_start_index = 0u;
        }
      }

      input_offset += current_num_rows_to_insert;
      start_index = 0u;
    }
  }
}

std::map<RowID, PartitionID> Partitioning::_map_content_to_add_to_partitions(
    std::shared_ptr<Table> source, std::shared_ptr<const AbstractPartitionSchema> target_partition_schema) {
  // TODO(partitioning group): refactor!
  std::map<RowID, PartitionID> target_partition_mapping;
  if (std::dynamic_pointer_cast<const RangePartitionSchema>(target_partition_schema)) {
    auto range_partition_schema = std::dynamic_pointer_cast<const RangePartitionSchema>(target_partition_schema);
    for (ChunkID chunkID = ChunkID{0}; chunkID < source->chunk_count(); ++chunkID) {
      const auto source_chunk = source->get_chunk(chunkID);
      auto column_with_partitioning_values = source_chunk->get_column(range_partition_schema->get_column_id());
      for (uint32_t rowID = 0; rowID < source_chunk->size(); ++rowID) {
        target_partition_mapping[{chunkID, rowID}] =
            range_partition_schema->get_matching_partition_for((*column_with_partitioning_values)[rowID]);
      }
    }
  } else if (std::dynamic_pointer_cast<const HashPartitionSchema>(target_partition_schema)) {
    auto hash_partition_schema = std::dynamic_pointer_cast<const HashPartitionSchema>(target_partition_schema);
    for (ChunkID chunkID = ChunkID{0}; chunkID < source->chunk_count(); ++chunkID) {
      const auto source_chunk = source->get_chunk(chunkID);
      auto column_with_partitioning_values = source_chunk->get_column(hash_partition_schema->get_column_id());
      for (uint32_t rowID = 0; rowID < source_chunk->size(); ++rowID) {
        target_partition_mapping[{chunkID, rowID}] =
            hash_partition_schema->get_matching_partition_for((*column_with_partitioning_values)[rowID]);
      }
    }
  } else if (std::dynamic_pointer_cast<const RoundRobinPartitionSchema>(target_partition_schema)) {
    auto round_robin_partition_schema =
        std::dynamic_pointer_cast<const RoundRobinPartitionSchema>(target_partition_schema);
    for (ChunkID chunkID = ChunkID{0}; chunkID < source->chunk_count(); ++chunkID) {
      const auto source_chunk = source->get_chunk(chunkID);
      for (uint32_t rowID = 0; rowID < source_chunk->size(); ++rowID) {
        target_partition_mapping[{chunkID, rowID}] = round_robin_partition_schema->get_next_partition();
      }
    }
  } else if (std::dynamic_pointer_cast<const NullPartitionSchema>(target_partition_schema)) {
    for (ChunkID chunkID = ChunkID{0}; chunkID < source->chunk_count(); ++chunkID) {
      const auto source_chunk = source->get_chunk(chunkID);
      for (uint32_t rowID = 0; rowID < source_chunk->size(); ++rowID) {
        target_partition_mapping[{chunkID, rowID}] = PartitionID{0};
      }
    }
  } else {
    throw std::runtime_error("Unknown partition schema!");
  }
  return target_partition_mapping;
}

std::map<PartitionID, uint32_t> Partitioning::_count_rows_for_partitions(
    std::map<RowID, PartitionID> target_partition_mapping) {
  std::map<PartitionID, uint32_t> rows_to_add_to_partition;
  for (std::map<RowID, PartitionID>::iterator iter = target_partition_mapping.begin();
       iter != target_partition_mapping.end(); ++iter) {
    PartitionID partitionID = iter->second;
    if (rows_to_add_to_partition.count(partitionID) == 1) {
      rows_to_add_to_partition[partitionID] += 1u;
    } else {
      rows_to_add_to_partition[partitionID] = 1u;
    }
  }
  return rows_to_add_to_partition;
}

void Partitioning::_replace_table(std::shared_ptr<Table> partitioned_table) {
  StorageManager::get().replace_table(_table_to_partition_name, partitioned_table);
}

}  // namespace opossum
