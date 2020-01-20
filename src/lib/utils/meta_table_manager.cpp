#include <chrono>
#include <thread>
#include <iostream>
#include <fstream>
#include "stdio.h"

#include "meta_table_manager.hpp"

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

MetaTableManager::MetaTableManager() {
  _methods["tables"] = &MetaTableManager::generate_tables_table;
  _methods["columns"] = &MetaTableManager::generate_columns_table;
  _methods["chunks"] = &MetaTableManager::generate_chunks_table;
  _methods["segments"] = &MetaTableManager::generate_segments_table;
  _methods["workload"] = &MetaTableManager::generate_workload_table;

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

std::shared_ptr<Table> MetaTableManager::generate_segments_table() {
  const auto columns = TableColumnDefinitions{{"table_name", DataType::String, false},
                                              {"chunk_id", DataType::Int, false},
                                              {"column_id", DataType::Int, false},
                                              {"column_name", DataType::String, false},
                                              {"column_data_type", DataType::String, false},
                                              {"encoding_name", DataType::String, true}};
  // Vector compression is not yet included because #1286 makes it a pain to map it to a string.
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
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

std::shared_ptr<Table> MetaTableManager::generate_workload_table() {
  const auto columns = TableColumnDefinitions{{"cpu_total_used", DataType::Float, false},
                                              {"cpu_process_used", DataType::Float, false},
                                              {"ram_available", DataType::Int, false},
                                              {"ram_total_used", DataType::Int, false},
                                              {"ram_process_used", DataType::Int, false}};

  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  // delay between two cpu time measurements
  auto time_window = std::chrono::milliseconds(1000);

#if defined(__unix__) || defined(__unix) || defined(unix)
  // Linux

  // get system wide cpu usage
  std::ifstream stat_file;
  std::string cpu_line;

  stat_file.open("/proc/stat", std::ifstream::in);
  std::getline(stat_file, cpu_line);
  unsigned long long total_user_ref, total_user_nice_ref, total_system_ref, total_idle_ref;
  std::sscanf(cpu_line.c_str(), "cpu %llu %llu %llu %llu", &total_user_ref, &total_user_nice_ref, &total_system_ref, &total_idle_ref);
  stat_file.close();
  
  std::this_thread::sleep_for(time_window);
  
  stat_file.open("/proc/stat", std::ifstream::in);
  std::getline(stat_file, cpu_line);
  unsigned long long total_user, total_user_nice, total_system, total_idle;
  std::sscanf(cpu_line.c_str(), "cpu %llu %llu %llu %llu", &total_user, &total_user_nice, &total_system, &total_idle);
  stat_file.close();

  auto used = (total_user - total_user_ref) + (total_user_nice - total_user_nice_ref) + (total_system - total_system_ref);
  auto total = used + (total_idle - total_idle_ref);

  float system_cpu_usage = 100.0 * used / total;

  // get process cpu usage
  std::ifstream self_stat_file;
  std::string self_stat_token;
  self_stat_file.open("/proc/self/stat", std::ifstream::in);
  for (int field_index = 0; field_index < 13; ++field_index) {
    std::getline(self_stat_file, self_stat_token, ' ');
  }
  unsigned long long user_time_ref;
  std::sscanf(self_stat_token.c_str(), "%llu", &user_time_ref);

  std::getline(self_stat_file, self_stat_token, ' ');
  unsigned long long kernel_time_ref;
  std::sscanf(self_stat_token.c_str(), "%llu", &kernel_time_ref);

  std::this_thread::sleep_for(time_window);

  self_stat_file.open("/proc/self/stat", std::ifstream::in);
  for (int field_index = 0; field_index < 13; ++field_index) {
    std::getline(self_stat_file, self_stat_token, ' ');
  }
  unsigned long long user_time;
  std::sscanf(self_stat_token.c_str(), "%llu", &user_time);

  std::getline(self_stat_file, self_stat_token, ' ');
  unsigned long long kernel_time;
  std::sscanf(self_stat_token.c_str(), "%llu", &kernel_time);



  output_table->append({system_cpu_usage, float{0}, int32_t{0}, int32_t{0}, int32_t{0}});
  return output_table;

#else 
  // undefined OS
  return output_table;
#endif
}

bool MetaTableManager::is_meta_table_name(const std::string& name) {
  const auto prefix_len = META_PREFIX.size();
  return name.size() > prefix_len && std::string_view{&name[0], prefix_len} == MetaTableManager::META_PREFIX;
}

}  // namespace opossum
