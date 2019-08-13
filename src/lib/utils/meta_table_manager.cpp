#include "meta_table_manager.hpp"

#include "constant_mappings.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

MetaTableManager::MetaTableManager() {
  _methods["tables"] = std::bind(&MetaTableManager::generate_tables_table, this);
  _methods["columns"] = std::bind(&MetaTableManager::generate_columns_table, this);
  _methods["chunks"] = std::bind(&MetaTableManager::generate_chunks_table, this);
  _methods["segments"] = std::bind(&MetaTableManager::generate_segments_table, this);

  _table_names.reserve(_methods.size());
  for (const auto& [table_name, _] : _methods) {
    _table_names.emplace_back(table_name);
  }
  std::sort(_table_names.begin(), _table_names.end());
}

const std::vector<std::string>& MetaTableManager::table_names() const {
  return _table_names;
}

std::shared_ptr<Table> MetaTableManager::generate_table(const std::string& table_name) const {
  return _methods.at(table_name)();
}

std::shared_ptr<Table> MetaTableManager::generate_tables_table() const {
  const auto columns = TableColumnDefinitions{{"table", DataType::String, false},
                                              {"column_count", DataType::Int, false},
                                              {"row_count", DataType::Long, false},
                                              {"chunk_count", DataType::Int, false},
                                              {"max_chunk_size", DataType::Long, false}};
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : StorageManager::get().tables()) {
    output_table->append({pmr_string{table_name}, static_cast<int32_t>(table->column_count()),
                          static_cast<int64_t>(table->row_count()), static_cast<int32_t>(table->chunk_count()),
                          static_cast<int64_t>(table->max_chunk_size())});
  }

  return output_table;
}

std::shared_ptr<Table> MetaTableManager::generate_columns_table() const {
  const auto columns = TableColumnDefinitions{{"table", DataType::String, false},
                                              {"name", DataType::String, false},
                                              {"type", DataType::String, false},
                                              {"nullable", DataType::Int, false}};
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : StorageManager::get().tables()) {
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      output_table->append({pmr_string{table_name}, static_cast<pmr_string>(table->column_name(column_id)),
                            static_cast<pmr_string>(data_type_to_string.left.at(table->column_data_type(column_id))),
                            static_cast<int32_t>(table->column_is_nullable(column_id))});
    }
  }

  return output_table;
}

std::shared_ptr<Table> MetaTableManager::generate_chunks_table() const {
  const auto columns = TableColumnDefinitions{{"table", DataType::String, false},
                                              {"chunk_id", DataType::Int, false},
                                              {"rows", DataType::Long, false},
                                              {"invalid_rows", DataType::Long, false},
                                              {"cleanup_commit_id", DataType::Long, true}};
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : StorageManager::get().tables()) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      const auto cleanup_commit_id = chunk->get_cleanup_commit_id()
                                         ? AllTypeVariant{static_cast<int64_t>(*chunk->get_cleanup_commit_id())}
                                         : NULL_VALUE;
      output_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int64_t>(chunk->size()),
                            static_cast<int64_t>(chunk->invalid_row_count()), cleanup_commit_id});
    }
  }

  return output_table;
}

std::shared_ptr<Table> MetaTableManager::generate_segments_table() const {
  // TODO column_name/_type violate 3NF, do we want to include them for convenience?

  const auto columns =
      TableColumnDefinitions{{"table", DataType::String, false},       {"chunk_id", DataType::Int, false},
                             {"column_id", DataType::Int, false},      {"column_name", DataType::String, false},
                             {"column_type", DataType::String, false}, {"encoding", DataType::String, true}};
  // Vector compression is not yet included because #1286 makes it a pain to map it to a string.
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : StorageManager::get().tables()) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(column_id);

        const auto data_type = pmr_string{data_type_to_string.left.at(table->column_data_type(column_id))};
        AllTypeVariant encoding = NULL_VALUE;
        if (const auto& encoded_segment = std::dynamic_pointer_cast<BaseEncodedSegment>(segment)) {
          encoding = pmr_string{encoding_type_to_string.left.at(encoded_segment->encoding_type())};
        }

        output_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, encoding});
      }
    }
  }

  return output_table;
}

}  // namespace opossum
