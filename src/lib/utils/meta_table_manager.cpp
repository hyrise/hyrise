#include "meta_table_manager.hpp"

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace {

using namespace opossum;  // NOLINT

size_t get_distinct_value_count(const std::shared_ptr<BaseSegment>& segment) {
  auto distinct_value_count = size_t{0};
  resolve_data_type(segment->data_type(), [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    // For dictionary segments, an early (and much faster) exit is possible by using the dictionary size
    if (const auto dictionary_segment = std::dynamic_pointer_cast<const DictionarySegment<ColumnDataType>>(segment)) {
      distinct_value_count = dictionary_segment->dictionary()->size();
      return;
    } else if (const auto fs_dictionary_segment = std::dynamic_pointer_cast<const FixedStringDictionarySegment<pmr_string>>(segment)) {
      distinct_value_count = fs_dictionary_segment->dictionary()->size();
      return;
    }

    std::unordered_set<ColumnDataType> distinct_values;
    auto iterable = create_any_segment_iterable<ColumnDataType>(*segment);
    iterable.with_iterators([&](auto it, auto end) {
      for (; it != end; ++it) {
        const auto segment_item = *it;
        if (!segment_item.is_null()) {
          distinct_values.insert(segment_item.value());
        }
      }
    });
    distinct_value_count = distinct_values.size();
  });
  return distinct_value_count;
}

auto gather_segment_meta_data(const std::shared_ptr<Table>& meta_table, const MemoryUsageCalculationMode mode) {
  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(column_id);

        const auto data_type = pmr_string{data_type_to_string.left.at(table->column_data_type(column_id))};
        const auto estimated_size = segment->memory_usage(mode);
        AllTypeVariant encoding = NULL_VALUE;
        AllTypeVariant vector_compression = NULL_VALUE;
        if (const auto& encoded_segment = std::dynamic_pointer_cast<BaseEncodedSegment>(segment)) {
          encoding = pmr_string{encoding_type_to_string.left.at(encoded_segment->encoding_type())};

          if (encoded_segment->compressed_vector_type()) {
            std::stringstream ss;
            ss << *encoded_segment->compressed_vector_type();
            vector_compression = pmr_string{ss.str()};
          }
        }

        if (mode == MemoryUsageCalculationMode::Full) {
          const auto distinct_value_count = static_cast<int32_t>(get_distinct_value_count(segment));
          meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, distinct_value_count, encoding,
                              vector_compression, static_cast<int32_t>(estimated_size)});
        } else {
          meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, encoding, vector_compression,
                              static_cast<int32_t>(estimated_size)});
        }
      }
    }
  }
}

}  // anonymous namespace

namespace opossum {

MetaTableManager::MetaTableManager() {
  _methods["tables"] = &MetaTableManager::generate_tables_table;
  _methods["columns"] = &MetaTableManager::generate_columns_table;
  _methods["chunks"] = &MetaTableManager::generate_chunks_table;
  _methods["chunk_sort_orders"] = &MetaTableManager::generate_chunk_sort_orders_table;
  _methods["single_column_indexes"] = &MetaTableManager::generate_single_column_indexes_table;
  _methods["segments"] = &MetaTableManager::generate_segments_table;
  _methods["segments_accurate"] = &MetaTableManager::generate_accurate_segments_table;

  _table_names.reserve(_methods.size());
  for (const auto& [table_name, _] : _methods) {
    _table_names.emplace_back(table_name);
  }
  std::sort(_table_names.begin(), _table_names.end());
}

const std::vector<std::string>& MetaTableManager::table_names() const { return _table_names; }

std::shared_ptr<Table> MetaTableManager::generate_table(const std::string& table_name) const {
  const auto table = _methods.at(table_name)();
  table->set_table_statistics(TableStatistics::from_table(*table));
  return table;
}

std::shared_ptr<Table> MetaTableManager::generate_tables_table() {
  const auto columns = TableColumnDefinitions{{"table_name", DataType::String, false},
                                              {"column_count", DataType::Int, false},
                                              {"row_count", DataType::Long, false},
                                              {"chunk_count", DataType::Int, false},
                                              {"max_chunk_size", DataType::Int, false}};
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    output_table->append({pmr_string{table_name}, static_cast<int32_t>(table->column_count()),
                          static_cast<int64_t>(table->row_count()), static_cast<int32_t>(table->chunk_count()),
                          static_cast<int32_t>(table->max_chunk_size())});
  }

  return output_table;
}

std::shared_ptr<Table> MetaTableManager::generate_columns_table() {
  const auto columns = TableColumnDefinitions{{"table_name", DataType::String, false},
                                              {"column_name", DataType::String, false},
                                              {"data_type", DataType::String, false},
                                              {"nullable", DataType::Int, false}};
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      output_table->append({pmr_string{table_name}, static_cast<pmr_string>(table->column_name(column_id)),
                            static_cast<pmr_string>(data_type_to_string.left.at(table->column_data_type(column_id))),
                            static_cast<int32_t>(table->column_is_nullable(column_id))});
    }
  }

  return output_table;
}

std::shared_ptr<Table> MetaTableManager::generate_chunks_table() {
  const auto columns = TableColumnDefinitions{{"table_name", DataType::String, false},
                                              {"chunk_id", DataType::Int, false},
                                              {"row_count", DataType::Long, false},
                                              {"invalid_row_count", DataType::Long, false},
                                              {"cleanup_commit_id", DataType::Long, true}};
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
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

std::shared_ptr<Table> MetaTableManager::generate_chunk_sort_orders_table() {
  const auto columns = TableColumnDefinitions{{"table_name", DataType::String, false},
                                              {"chunk_id", DataType::Int, false},
                                              {"column_id", DataType::Int, false},
                                              {"order_mode", DataType::String, false}};
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      const auto ordered_by = chunk->ordered_by();
      if (ordered_by) {
        std::stringstream order_by_mode_steam;
        order_by_mode_steam << ordered_by->second;
        output_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(ordered_by->first),
                            pmr_string{order_by_mode_steam.str()}});
      }
    }
  }

  return output_table;
}

std::shared_ptr<Table> MetaTableManager::generate_single_column_indexes_table() {
  // Each index has an artificial ID (not used within Hyrise) that is required to identify multi-column indexes
  const auto columns = TableColumnDefinitions{{"table_name", DataType::String, false},
                                              {"chunk_id", DataType::Int, false},
                                              {"index_id", DataType::Int, false},
                                              {"column_id", DataType::Int, false},
                                              {"index_type", DataType::String, false}};
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  // TODO(anyone): we should store the indexes that are available within each chunk. We have IndexStatistics for
  //     tables, but that would mean we can only handle the indexes that have been created for the whole table.
  //     As soon as we expect tables to grow over time, the current way of handling indexes needs to be adapted.
  //     Comment that shall not be merged: Martin votes for moving all IndexStatistics information to the chunks.
  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    const auto column_count = table->column_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        const auto& indexes = chunk->get_indexes({column_id});
        auto index_id = int32_t{0};
        for (const auto& index : indexes) {
          std::stringstream index_type_name;
          // TODO(anyone): use some form of index_type_name << index->type();
          if (std::dynamic_pointer_cast<const GroupKeyIndex>(index)) {
            index_type_name << "GroupKeyIndex";
          } else if (std::dynamic_pointer_cast<const CompositeGroupKeyIndex>(index)) {
            index_type_name << "CompositeGroupKeyIndex";
          } else if (std::dynamic_pointer_cast<const AdaptiveRadixTreeIndex>(index)) {
            index_type_name << "AdaptiveRadixTreeIndex";
          } else if (std::dynamic_pointer_cast<const BTreeIndex>(index)) {
            index_type_name << "BTreeIndex";
          } else {
            index_type_name << "Unknown";
          }

          output_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(index_id),
                               static_cast<int32_t>(column_id), pmr_string{index_type_name.str()}});

          ++index_id;
        }
      }
    }
  }

  return output_table;
}

std::shared_ptr<Table> MetaTableManager::generate_segments_table() {
  const auto columns = TableColumnDefinitions{{"table_name", DataType::String, false},
                                              {"chunk_id", DataType::Int, false},
                                              {"column_id", DataType::Int, false},
                                              {"column_name", DataType::String, false},
                                              {"column_data_type", DataType::String, false},
                                              {"encoding_type", DataType::String, true},
                                              {"vector_compression_type", DataType::String, true},
                                              {"estimated_size_in_bytes", DataType::Int, false}};

  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);
  gather_segment_meta_data(output_table, MemoryUsageCalculationMode::Sampled);

  return output_table;
}

std::shared_ptr<Table> MetaTableManager::generate_accurate_segments_table() {
  PerformanceWarning("Accurate segment information are expensive to gather. Use with caution.");
  const auto columns = TableColumnDefinitions{
      {"table_name", DataType::String, false},       {"chunk_id", DataType::Int, false},
      {"column_id", DataType::Int, false},           {"column_name", DataType::String, false},
      {"column_data_type", DataType::String, false}, {"distinct_value_count", DataType::Int, false},
      {"encoding_type", DataType::String, true},     {"vector_compression_type", DataType::String, true},
      {"size_in_bytes", DataType::Int, false}};

  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);
  gather_segment_meta_data(output_table, MemoryUsageCalculationMode::Full);

  return output_table;
}

bool MetaTableManager::is_meta_table_name(const std::string& name) {
  const auto prefix_len = META_PREFIX.size();
  return name.size() > prefix_len && std::string_view{&name[0], prefix_len} == MetaTableManager::META_PREFIX;
}

}  // namespace opossum
