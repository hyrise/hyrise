#include <chrono>
#include <thread>
#include <iostream>
#include <fstream>
#include "stdio.h"
#include "sys/types.h"
#include "sys/sysinfo.h"
#include "sys/times.h"

#include "meta_table_manager.hpp"

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace {

using namespace opossum;  // NOLINT

// TODO(anyone): #1968 introduced this namespace. With the expected growth of the meta table manager of time, there
//               might be a large number of helper function that are only loosely related to the core functionality
//               of the MetaTableManager. If this becomes the case, restructure and move the functions to other files.
size_t get_distinct_value_count(const std::shared_ptr<BaseSegment>& segment) {
  auto distinct_value_count = size_t{0};
  resolve_data_type(segment->data_type(), [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    // For dictionary segments, an early (and much faster) exit is possible by using the dictionary size
    if (const auto dictionary_segment = std::dynamic_pointer_cast<const DictionarySegment<ColumnDataType>>(segment)) {
      distinct_value_count = dictionary_segment->dictionary()->size();
      return;
    } else if (const auto fs_dictionary_segment =
                   std::dynamic_pointer_cast<const FixedStringDictionarySegment<pmr_string>>(segment)) {
      distinct_value_count = fs_dictionary_segment->fixed_string_dictionary()->size();
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
          const auto distinct_value_count = static_cast<int64_t>(get_distinct_value_count(segment));
          meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, distinct_value_count, encoding,
                              vector_compression, static_cast<int64_t>(estimated_size)});
        } else {
          meta_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int32_t>(column_id),
                              pmr_string{table->column_name(column_id)}, data_type, encoding, vector_compression,
                              static_cast<int64_t>(estimated_size)});
        }
      }
    }
  }
}

std::vector<float> get_load_avg() {
#if defined(__linux__)

  std::ifstream load_avg_file;
  load_avg_file.open("/proc/loadavg", std::ifstream::in);
  
  std::string load_avg_value;
  std::vector<float> load_avg_values;
  for (int value_index = 0; value_index < 3; ++value_index){
    std::getline(load_avg_file, load_avg_value, ' ');
    load_avg_values.push_back(std::stof(load_avg_value));
  }
  load_avg_file.close();
  
  return load_avg_values;

#endif

  Fail("Can't read /proc/loadavg on non-Linux systems");
}

float get_system_cpu_usage() {
#if defined(__linux__)

  static unsigned long long last_user_time = 0u, last_user_nice_time = 0u, last_kernel_time = 0u, last_idle_time = 0u;
  
  std::ifstream stat_file;
  stat_file.open("/proc/stat", std::ifstream::in);
  
  std::string cpu_line;
  std::getline(stat_file, cpu_line);
  unsigned long long user_time, user_nice_time, kernel_time, idle_time;
  std::sscanf(cpu_line.c_str(), "cpu %llu %llu %llu %llu", &user_time, &user_nice_time, &kernel_time, &idle_time);
  stat_file.close();
  
  auto used = (user_time - last_user_time) + (user_nice_time - last_user_nice_time) + (kernel_time - last_kernel_time);
  auto total = used + (idle_time - last_idle_time);

  last_user_time = user_time;
  last_user_nice_time = user_nice_time;
  last_kernel_time = kernel_time;
  last_idle_time = idle_time;

  return 100.0 * used / total;

#endif

  Fail("Can't read /proc/stat on non-Linux systems");  
}

float get_process_cpu_usage() {
#if defined(__linux__)

  static clock_t last_clock_time = 0u, last_kernel_time = 0u, last_user_time = 0u;
  struct tms timeSample;

  auto clock_time = times(&timeSample);
  auto kernel_time = timeSample.tms_stime;
  auto user_time = timeSample.tms_utime;

  auto used = (user_time - last_user_time) + (kernel_time - last_kernel_time);
  auto total = clock_time - last_clock_time;

  last_user_time = user_time;
  last_kernel_time = kernel_time;
  last_clock_time = clock_time;

  // TODO divide by processors
  return 100.0 * used / total;

#endif

  Fail("Can't use sys/times.h on non-Linux systems");  
}

std::vector<int> get_ram_usage() {
#if defined(__linux__)

  struct sysinfo memory_info;
  sysinfo(&memory_info);

  auto total_ram = memory_info.totalram * memory_info.mem_unit;
  auto total_swap = memory_info.totalswap * memory_info.mem_unit;
  auto ram_used = (memory_info.totalram - memory_info.freeram) * memory_info.mem_unit;
  auto swap_used = (memory_info.totalswap - memory_info.freeswap) * memory_info.mem_unit;

  return {static_cast<int>(total_ram), static_cast<int>(total_swap), static_cast<int>(ram_used), static_cast<int>(swap_used)};

#endif

  Fail("Can't use sys/sysinfo.h on non-Linux systems");  
}

auto gather_workload_meta_data(const std::shared_ptr<Table>& meta_table) {

#if defined(__linux__)
  auto system_cpu_usage = get_system_cpu_usage();
  auto process_cpu_usage = get_process_cpu_usage();
  auto load_avg = get_load_avg();
  auto ram_usage = get_ram_usage();

  meta_table->append({system_cpu_usage, process_cpu_usage, load_avg.at(0), load_avg.at(1), load_avg.at(2), ram_usage.at(2), ram_usage.at(3)});

#endif
}

}  // namespace

namespace opossum {

MetaTableManager::MetaTableManager() {
  _methods["tables"] = &MetaTableManager::generate_tables_table;
  _methods["columns"] = &MetaTableManager::generate_columns_table;
  _methods["chunks"] = &MetaTableManager::generate_chunks_table;
  _methods["chunk_sort_orders"] = &MetaTableManager::generate_chunk_sort_orders_table;
  _methods["segments"] = &MetaTableManager::generate_segments_table;
  _methods["workload"] = &MetaTableManager::generate_workload_table;
  _methods["segments_accurate"] = &MetaTableManager::generate_accurate_segments_table;

  _table_names.reserve(_methods.size());
  for (const auto& [table_name, _] : _methods) {
    _table_names.emplace_back(table_name);
  }
  std::sort(_table_names.begin(), _table_names.end());
  
  get_system_cpu_usage();
  get_process_cpu_usage();
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
                                              {"max_chunk_size", DataType::Long, false}};
  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    output_table->append({pmr_string{table_name}, static_cast<int32_t>(table->column_count()),
                          static_cast<int64_t>(table->row_count()), static_cast<int32_t>(table->chunk_count()),
                          static_cast<int64_t>(table->max_chunk_size())});
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

/**
 * At the moment, each chunk can be sorted by exactly one column or none. Hence, having a column within the chunk table
 * would be sufficient. However, this will change in the near future (e.g., when a sort-merge join evicts a chunk that
 * is sorted on two columns). To prepare for this change, this additional table stores the sort orders and allows a
 * chunk to have multiple sort orders. Cascading sort orders for chunks are currently not planned.
 */
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
        output_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id),
                              static_cast<int32_t>(ordered_by->first), pmr_string{order_by_mode_steam.str()}});
      }
    }
  }

  return output_table;
}

std::shared_ptr<Table> MetaTableManager::generate_workload_table() {
  const auto columns = TableColumnDefinitions{{"cpu_system_usage", DataType::Float, false},
                                              {"cpu_process_usage", DataType::Float, false},
                                              {"load_average_1_min", DataType::Float, false},
                                              {"load_average_5_min", DataType::Float, false},
                                              {"load_average_15_min", DataType::Float, false},
                                              {"ram_used", DataType::Int, false},
                                              {"swap_used", DataType::Int, false}};//,
                                              //{"ram_used_percent", DataType::Float, false},
                                              //{"swap_used_percent", DataType::Float, false},
                                              //{"mem_used_percent", DataType::Float, false}};

  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);

  gather_workload_meta_data(output_table);
  
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
                                              {"estimated_size_in_bytes", DataType::Long, false}};

  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);
  gather_segment_meta_data(output_table, MemoryUsageCalculationMode::Sampled);

  return output_table;
}

std::shared_ptr<Table> MetaTableManager::generate_accurate_segments_table() {
  PerformanceWarning("Accurate segment information are expensive to gather. Use with caution.");
  const auto columns = TableColumnDefinitions{
      {"table_name", DataType::String, false},       {"chunk_id", DataType::Int, false},
      {"column_id", DataType::Int, false},           {"column_name", DataType::String, false},
      {"column_data_type", DataType::String, false}, {"distinct_value_count", DataType::Long, false},
      {"encoding_type", DataType::String, true},     {"vector_compression_type", DataType::String, true},
      {"size_in_bytes", DataType::Long, false}};

  auto output_table = std::make_shared<Table>(columns, TableType::Data, std::nullopt, UseMvcc::Yes);
  gather_segment_meta_data(output_table, MemoryUsageCalculationMode::Full);

  return output_table;
}

bool MetaTableManager::is_meta_table_name(const std::string& name) {
  const auto prefix_len = META_PREFIX.size();
  return name.size() > prefix_len && std::string_view{&name[0], prefix_len} == MetaTableManager::META_PREFIX;
}

}  // namespace opossum
