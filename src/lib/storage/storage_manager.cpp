#include "storage_manager.hpp"

#include <sys/fcntl.h>
#include <sys/mman.h>
#include <numeric>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "import_export/binary/binary_writer.hpp"
#include "import_export/file_type.hpp"
#include "operators/export.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/value_segment.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"


uint32_t byte_index(uint32_t element_index, size_t element_size) {
  return element_index * element_size;
}

uint32_t element_index(uint32_t byte_index, size_t element_size) {
  return byte_index / element_size;
}

namespace hyrise {

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  const auto table_iter = _tables.find(name);
  const auto view_iter = _views.find(name);
  Assert(table_iter == _tables.end() || !table_iter->second,
         "Cannot add table " + name + " - a table with the same name already exists");
  Assert(view_iter == _views.end() || !view_iter->second,
         "Cannot add table " + name + " - a view with the same name already exists");

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    // We currently assume that all tables stored in the StorageManager are mutable and, as such, have MVCC data. This
    // way, we do not need to check query plans if they try to update immutable tables. However, this is not a hard
    // limitation and might be changed into more fine-grained assertions if the need arises.
    Assert(table->get_chunk(chunk_id)->has_mvcc_data(), "Table must have MVCC data.");
  }

  // Create table statistics and chunk pruning statistics for added table.

  table->set_table_statistics(TableStatistics::from_table(*table));
  generate_chunk_pruning_statistics(table);

  _tables[name] = std::move(table);

  auto table_persistence_file_name = name + "_0.bin";
  _tables_current_persistence_file_mapping[name] = {table_persistence_file_name, 0, 0};
}

void StorageManager::drop_table(const std::string& name) {
  const auto table_iter = _tables.find(name);
  Assert(table_iter != _tables.end() && table_iter->second, "Error deleting table. No such table named '" + name + "'");

  // The concurrent_unordered_map does not support concurrency-safe erasure. Thus, we simply reset the table pointer.
  _tables[name] = nullptr;
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  const auto table_iter = _tables.find(name);
  Assert(table_iter != _tables.end(), "No such table named '" + name + "'");

  auto table = table_iter->second;
  Assert(table,
         "Nullptr found when accessing table named '" + name + "'. This can happen if a dropped table is accessed.");

  return table;
}

bool StorageManager::has_table(const std::string& name) const {
  const auto table_iter = _tables.find(name);
  return table_iter != _tables.end() && table_iter->second;
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  table_names.reserve(_tables.size());

  for (const auto& table_item : _tables) {
    if (!table_item.second) {
      continue;
    }

    table_names.emplace_back(table_item.first);
  }

  return table_names;
}

std::unordered_map<std::string, std::shared_ptr<Table>> StorageManager::tables() const {
  std::unordered_map<std::string, std::shared_ptr<Table>> result;

  for (const auto& [table_name, table] : _tables) {
    // Skip dropped table, as we don't remove the map entry when dropping, but only reset the table pointer.
    if (!table) {
      continue;
    }

    result[table_name] = table;
  }

  return result;
}

void StorageManager::add_view(const std::string& name, const std::shared_ptr<LQPView>& view) {
  const auto table_iter = _tables.find(name);
  const auto view_iter = _views.find(name);
  Assert(table_iter == _tables.end() || !table_iter->second,
         "Cannot add view " + name + " - a table with the same name already exists");
  Assert(view_iter == _views.end() || !view_iter->second,
         "Cannot add view " + name + " - a view with the same name already exists");

  _views[name] = view;
}

void StorageManager::drop_view(const std::string& name) {
  const auto view_iter = _views.find(name);
  Assert(view_iter != _views.end() && view_iter->second, "Error deleting view. No such view named '" + name + "'");

  _views[name] = nullptr;
}

std::shared_ptr<LQPView> StorageManager::get_view(const std::string& name) const {
  const auto view_iter = _views.find(name);
  Assert(view_iter != _views.end(), "No such view named '" + name + "'");

  const auto view = view_iter->second;
  Assert(view,
         "Nullptr found when accessing view named '" + name + "'. This can happen if a dropped view is accessed.");

  return view->deep_copy();
}

bool StorageManager::has_view(const std::string& name) const {
  const auto view_iter = _views.find(name);
  return view_iter != _views.end() && view_iter->second;
}

std::vector<std::string> StorageManager::view_names() const {
  std::vector<std::string> view_names;
  view_names.reserve(_views.size());

  for (const auto& view_item : _views) {
    if (!view_item.second) {
      continue;
    }

    view_names.emplace_back(view_item.first);
  }

  return view_names;
}

std::unordered_map<std::string, std::shared_ptr<LQPView>> StorageManager::views() const {
  std::unordered_map<std::string, std::shared_ptr<LQPView>> result;

  for (const auto& [view_name, view] : _views) {
    if (!view) {
      continue;
    }

    result[view_name] = view;
  }

  return result;
}

void StorageManager::add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan) {
  const auto iter = _prepared_plans.find(name);
  Assert(iter == _prepared_plans.end() || !iter->second,
         "Cannot add prepared plan " + name + " - a prepared plan with the same name already exists");

  _prepared_plans[name] = prepared_plan;
}

std::shared_ptr<PreparedPlan> StorageManager::get_prepared_plan(const std::string& name) const {
  const auto iter = _prepared_plans.find(name);
  Assert(iter != _prepared_plans.end(), "No such prepared plan named '" + name + "'");

  auto prepared_plan = iter->second;
  Assert(prepared_plan, "Nullptr found when accessing prepared plan named '" + name +
                            "'. This can happen if a dropped prepared plan is accessed.");

  return prepared_plan;
}

bool StorageManager::has_prepared_plan(const std::string& name) const {
  const auto iter = _prepared_plans.find(name);
  return iter != _prepared_plans.end() && iter->second;
}

void StorageManager::drop_prepared_plan(const std::string& name) {
  const auto iter = _prepared_plans.find(name);
  Assert(iter != _prepared_plans.end() && iter->second,
         "Error deleting prepared plan. No such prepared plan named '" + name + "'");

  _prepared_plans[name] = nullptr;
}

std::unordered_map<std::string, std::shared_ptr<PreparedPlan>> StorageManager::prepared_plans() const {
  std::unordered_map<std::string, std::shared_ptr<PreparedPlan>> result;

  for (const auto& [prepared_plan_name, prepared_plan] : _prepared_plans) {
    if (!prepared_plan) {
      continue;
    }

    result[prepared_plan_name] = prepared_plan;
  }

  return result;
}

void StorageManager::export_all_tables_as_csv(const std::string& path) {
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.reserve(_tables.size());

  for (const auto& table_item : _tables) {
    if (!table_item.second) {
      continue;
    }

    auto job_task = std::make_shared<JobTask>([table_item, &path]() {
      const auto& name = table_item.first;
      const auto& table = table_item.second;

      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->execute();

      auto export_csv = std::make_shared<Export>(table_wrapper, path + "/" + name + ".csv", FileType::Csv);  // NOLINT
      export_csv->execute();
    });
    tasks.push_back(job_task);
    job_task->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(tasks);
}

std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager) {
  stream << "==================" << std::endl;
  stream << "===== Tables =====" << std::endl << std::endl;

  for (auto const& table : storage_manager.tables()) {
    stream << "==== table >> " << table.first << " <<";
    stream << " (" << table.second->column_count() << " columns, " << table.second->row_count() << " rows in "
           << table.second->chunk_count() << " chunks)";
    stream << std::endl;
  }

  stream << "==================" << std::endl;
  stream << "===== Views ======" << std::endl << std::endl;

  for (auto const& view : storage_manager.views()) {
    stream << "==== view >> " << view.first << " <<";
    stream << std::endl;
  }

  stream << "==================" << std::endl;
  stream << "= PreparedPlans ==" << std::endl << std::endl;

  for (auto const& prepared_plan : storage_manager.prepared_plans()) {
    stream << "==== prepared plan >> " << prepared_plan.first << " <<";
    stream << std::endl;
  }

  return stream;
}

std::vector<uint32_t> StorageManager::calculate_segment_offset_ends(const std::shared_ptr<Chunk> chunk) {
  const auto segment_count = chunk->column_count();
  auto segment_offset_ends = std::vector<uint32_t>(segment_count);

  auto offset_end = _chunk_header_bytes(segment_count);
  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    const auto abstract_segment = chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(segment_index)));

    resolve_data_type(abstract_segment->data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if constexpr(std::is_same<ColumnDataType, pmr_string>::value) {
        offset_end += _segment_header_bytes + 4;
        const auto fixed_string_dict_segment = std::dynamic_pointer_cast<FixedStringDictionarySegment<ColumnDataType>>(abstract_segment);
        Assert(fixed_string_dict_segment, "Trying to map a non-FixedString String DictionarySegment");
        const auto fixed_dict_size = byte_index(fixed_string_dict_segment->fixed_string_dictionary()->size(), fixed_string_dict_segment->fixed_string_dictionary()->string_length());
        offset_end += fixed_dict_size;
        if ((fixed_dict_size % 4) != 0) {
          offset_end += (4 - (fixed_dict_size % 4));
        }
        offset_end += calculate_four_byte_aligned_size_of_attribute_vector(fixed_string_dict_segment->attribute_vector());
      } else {
        offset_end += _segment_header_bytes;
        const auto dict_segment = dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(abstract_segment);
        //TODO: is it safe to call sizeof(ColumnDataType) -> assert and print
        offset_end += byte_index(dict_segment->dictionary()->size(), sizeof(ColumnDataType));
        offset_end += calculate_four_byte_aligned_size_of_attribute_vector(dict_segment->attribute_vector());
      }
      segment_offset_ends[segment_index] = offset_end;
    });
  }

  return segment_offset_ends;
}

/*
 * Copied binary writing functions from `binary_writer.cpp`
 */

template <typename T>
void export_value(const T& value, std::string file_name) {
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(reinterpret_cast<const char*>(&value), sizeof(T));
  ofstream.close();
}

void export_values(const FixedStringVector& values, std::string file_name) {
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(values.data(), static_cast<int64_t>(values.size() * values.string_length()));
  ofstream.close();
}

// not copied, own creation
void overwrite_header(const FILE_HEADER header, std::string file_name) {
  std::fstream fstream(file_name, std::ios::binary); // use option std::ios_base::binary if necessary
  fstream.seekp(0, std::ios_base::beg);
  fstream.write(reinterpret_cast<const char*>(&header), sizeof(header));
  fstream.close();
}

template <typename T, typename Alloc>
void export_values(const std::vector<T, Alloc>& values, std::string file_name) {
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(T));
  ofstream.close();
}

// needed for attribute vector which is stored in a compact manner
void export_compact_vector(const pmr_compact_vector& values, std::string file_name) {
  //adapted to uint32_t format of later created map (see comment in `write_dict_segment_to_disk`)
  export_value(static_cast<uint32_t>(values.bits()), file_name);
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(reinterpret_cast<const char*>(values.get()), static_cast<int64_t>(values.bytes()));
  ofstream.close();
}

void export_compressed_vector(const CompressedVectorType type, const BaseCompressedVector& compressed_vector,
                              std::string file_name) {
  switch (type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
      export_values(dynamic_cast<const FixedWidthIntegerVector<uint32_t>&>(compressed_vector).data(), file_name);
      return;
    case CompressedVectorType::FixedWidthInteger2Byte:
      export_values(dynamic_cast<const FixedWidthIntegerVector<uint16_t>&>(compressed_vector).data(), file_name);
      return;
    case CompressedVectorType::FixedWidthInteger1Byte:
      export_values(dynamic_cast<const FixedWidthIntegerVector<uint8_t>&>(compressed_vector).data(), file_name);
      return;
    case CompressedVectorType::BitPacking:
      export_compact_vector(dynamic_cast<const BitPackingVector&>(compressed_vector).data(), file_name);
      return;
    default:
      Fail("Any other type should have been caught before.");
  }
}


template <typename T>
void StorageManager::write_fixed_string_dict_segment_to_disk(const std::shared_ptr<FixedStringDictionarySegment<T>> segment, const std::string& file_name) {
  const auto compressed_vector_type_id = resolve_persisted_segment_encoding_type_from_compression_type(segment->compressed_vector_type().value());
  export_value(static_cast<uint32_t>(compressed_vector_type_id), file_name);
  export_value(static_cast<uint32_t>(segment->fixed_string_dictionary()->string_length()), file_name);
  export_value(static_cast<uint32_t>(segment->fixed_string_dictionary()->size()), file_name);
  export_value(static_cast<uint32_t>(segment->attribute_vector()->size()), file_name);

  // we need to ensure that every part can be mapped with a uint32_t map
  export_values(*segment->fixed_string_dictionary(), file_name);
  const auto dictionary_byte_size = byte_index(segment->fixed_string_dictionary()->size(), segment->fixed_string_dictionary()->string_length());
  //TODO: use chars.size() instead?
  std::cout << dictionary_byte_size << " " << segment->fixed_string_dictionary()->chars().size() << std::endl;
  const auto dictionary_difference_to_four_byte_alignment = dictionary_byte_size % 4;
  if (dictionary_difference_to_four_byte_alignment != 0) {
    const auto padding = std::vector<uint8_t>(4 - dictionary_difference_to_four_byte_alignment, 0);
    export_values(padding, file_name);
  }

  export_compressed_vector(*segment->compressed_vector_type(), *segment->attribute_vector(), file_name);
  //TODO: What to do with non-compressed AttributeVectors?
  const auto attribute_vector_difference_to_four_byte_alignment = calculate_byte_size_of_attribute_vector(segment->attribute_vector()) % 4;
  if (attribute_vector_difference_to_four_byte_alignment != 0) {
  const auto padding = std::vector<uint8_t>(4 - attribute_vector_difference_to_four_byte_alignment, 0);
  export_values(padding, file_name);
  }
}

template <typename T>
void StorageManager::write_dict_segment_to_disk(const std::shared_ptr<DictionarySegment<T>> segment,
                                                const std::string& file_name) {
  /*
   * For a description of how dictionary segments look, see the following PR:
   *    https://github.com/hyrise-mp-22-23/hyrise/pull/94
   */
  const auto compressed_vector_type_id = resolve_persisted_segment_encoding_type_from_compression_type(segment->compressed_vector_type().value());
  export_value(static_cast<uint32_t>(compressed_vector_type_id), file_name);
  export_value(static_cast<uint32_t>(segment->dictionary()->size()), file_name);
  export_value(static_cast<uint32_t>(segment->attribute_vector()->size()), file_name);

  // we need to ensure that every part can be mapped with a uint32_t map
  export_values<T>(*segment->dictionary(), file_name);
  const auto dictionary_difference_to_four_byte_alignment = (segment->dictionary()->size() * sizeof(T)) % 4;
  if (dictionary_difference_to_four_byte_alignment != 0) {
    const auto padding = std::vector<uint8_t>(4 - dictionary_difference_to_four_byte_alignment, 0);
    export_values(padding, file_name);
  }

  export_compressed_vector(*segment->compressed_vector_type(), *segment->attribute_vector(), file_name);
  //TODO: What to do with non-compressed AttributeVectors?
  const auto attribute_vector_difference_to_four_byte_alignment = calculate_byte_size_of_attribute_vector(segment->attribute_vector()) % 4;
  if (attribute_vector_difference_to_four_byte_alignment != 0) {
    const auto padding = std::vector<uint8_t>(4 - attribute_vector_difference_to_four_byte_alignment, 0);
    export_values(padding, file_name);
  }
}

uint32_t calculate_byte_size_of_attribute_vector(std::shared_ptr<const BaseCompressedVector> attribute_vector) {
  const auto compressed_vector_type = attribute_vector->type();
  auto size = uint32_t{};
  switch (compressed_vector_type) {
    case CompressedVectorType::FixedWidthInteger1Byte:
      size = attribute_vector->size();
      break;
    case CompressedVectorType::FixedWidthInteger2Byte:
      size = attribute_vector->size() * 2;
      break;
    case CompressedVectorType::FixedWidthInteger4Byte:
      size = attribute_vector->size() * 4;
      break;
    case CompressedVectorType::BitPacking:
      size += 4;
      size += dynamic_cast<const BitPackingVector&>(*attribute_vector).data().bytes();
      break;
    default:
      Fail("Unknown Compression Type in Storage Manager.");
  }
  return size;
}

uint32_t calculate_four_byte_aligned_size_of_attribute_vector(std::shared_ptr<const BaseCompressedVector> attribute_vector) {
  const auto attribute_vector_size = calculate_byte_size_of_attribute_vector(attribute_vector);
  const auto attribute_vector_difference_to_four_byte_alignment = attribute_vector_size % 4;
  return attribute_vector_size + (4 - attribute_vector_difference_to_four_byte_alignment);
}

void StorageManager::write_segment_to_disk(const std::shared_ptr<AbstractSegment> abstract_segment, const std::string& file_name) {
  resolve_data_type(abstract_segment->data_type(), [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    if constexpr(std::is_same<ColumnDataType, pmr_string>::value) {
      const auto fixed_string_dict_segment = dynamic_pointer_cast<FixedStringDictionarySegment<ColumnDataType>>(abstract_segment);
      write_fixed_string_dict_segment_to_disk(fixed_string_dict_segment, file_name);
    } else {
      const auto dict_segment = dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(abstract_segment);
      write_dict_segment_to_disk(dict_segment, file_name);
    }
  });

}

void StorageManager::write_chunk_to_disk(const std::shared_ptr<Chunk>& chunk,
                                         const std::vector<uint32_t>& segment_offset_ends,
                                         const std::string& file_name) {
  auto header = CHUNK_HEADER{};
  header.row_count = chunk->size();
  header.segment_offset_ends = segment_offset_ends;

  export_value(header.row_count, file_name);

  for (const auto segment_offset_end : header.segment_offset_ends) {
    export_value(segment_offset_end, file_name);
  }

  const auto segment_count = chunk->column_count();
  for (auto segment_index = ColumnID{0}; segment_index < segment_count; ++segment_index) {
    const auto abstract_segment = chunk->get_segment(segment_index);
    write_segment_to_disk(abstract_segment, file_name);
  }
}

void StorageManager::persist_chunks_to_disk(const std::vector<std::shared_ptr<Chunk>>& chunks,
                                            const std::string& file_name) {
  /*
    TODO(everyone): Think about a proper implementation of a locking method. Each written file needs to be
    locked prior to writing (and released afterwards). It was decided to use the mutex class by cpp.
    (see https://en.cppreference.com/w/cpp/thread/mutex).
  */
  // file_lock.acquire();

  auto chunk_segment_offset_ends =
      std::vector<std::vector<uint32_t>>(StorageManager::_chunk_count, std::vector<uint32_t>());
  auto chunk_offset_ends = std::array<uint32_t, StorageManager::_chunk_count>();
  auto chunk_ids = std::array<uint32_t, StorageManager::_chunk_count>();

  auto offset = uint32_t{_file_header_bytes};
  for (auto chunk_index = uint32_t{0}; chunk_index < chunks.size(); ++chunk_index) {
    const auto segment_offset_ends = calculate_segment_offset_ends(chunks[chunk_index]);
    offset += segment_offset_ends.back();

    chunk_segment_offset_ends[chunk_index] = segment_offset_ends;
    chunk_offset_ends[chunk_index] = offset;
  }
  // Fill all offset fields, that are not used with 0s.
  for (auto chunk_index = chunks.size(); chunk_index < StorageManager::_chunk_count; ++chunk_index) {
    chunk_offset_ends[chunk_index] = uint32_t{0};
  }

  // TODO(everyone): Find, how to get the actual chunk id.
  for (auto index = uint32_t{0}; index < StorageManager::_chunk_count; ++index) {
    chunk_ids[index] = index;
  }

  auto fh = FILE_HEADER{};
  fh.storage_format_version_id = _storage_format_version_id;
  fh.chunk_count = static_cast<uint32_t>(chunks.size());
  fh.chunk_ids = chunk_ids;
  fh.chunk_offset_ends = chunk_offset_ends;

  export_value<FILE_HEADER>(fh, file_name);

  for (auto chunk_index = uint32_t{0}; chunk_index < chunks.size(); ++chunk_index) {
    const auto chunk = chunks[chunk_index];
    write_chunk_to_disk(chunk, chunk_segment_offset_ends[chunk_index], file_name);
  }

  // file_lock.release();
}

uint32_t StorageManager::persist_chunk_to_file(const std::shared_ptr<Chunk> chunk, ChunkID chunk_id,
                                           const std::string& file_name) {

    if (std::filesystem::exists(file_name)) {
      //append to existing file

      auto chunk_segment_offset_ends = calculate_segment_offset_ends(chunk);
      auto chunk_offset_end = chunk_segment_offset_ends.back();

      // adapt and rewrite file header
      FILE_HEADER file_header = read_file_header(file_name);
      const auto file_header_previous_chunk_count = file_header.chunk_count;
      const auto file_prev_chunk_end_offset = file_header.chunk_offset_ends[file_header_previous_chunk_count - 1];

      file_header.chunk_count = file_header.chunk_count + 1;
      file_header.chunk_ids[file_header_previous_chunk_count] = chunk_id;
      file_header.chunk_offset_ends[file_header_previous_chunk_count] = file_prev_chunk_end_offset + chunk_offset_end;

      overwrite_header(file_header, file_name);

      write_chunk_to_disk(chunk, chunk_segment_offset_ends, file_name);
      return file_prev_chunk_end_offset;
    }

    // create new file
    auto chunk_segment_offset_ends = calculate_segment_offset_ends(chunk);
    auto chunk_offset_end = chunk_segment_offset_ends.back();

    auto fh = FILE_HEADER{};
    auto chunk_ids = std::array<uint32_t, MAX_CHUNK_COUNT_PER_FILE>();
    chunk_ids[0] = chunk_id;

    auto chunk_offset_ends = std::array<uint32_t, MAX_CHUNK_COUNT_PER_FILE>();
    chunk_offset_ends[0] = chunk_offset_end;

    fh.storage_format_version_id = _storage_format_version_id;
    fh.chunk_count = uint32_t{1};
    fh.chunk_ids = chunk_ids;
    fh.chunk_offset_ends = chunk_offset_ends;

    export_value<FILE_HEADER>(fh, file_name);

    write_chunk_to_disk(chunk, chunk_segment_offset_ends, file_name);

    return _file_header_bytes;
}

void evaluate_mapped_chunk(const std::shared_ptr<Chunk>& chunk, const std::shared_ptr<Chunk>& mapped_chunk) {
  const auto created_segment = chunk->get_segment(ColumnID{5});

  resolve_data_type(created_segment->data_type(), [&](auto segment_data_type) {
    using SegmentDataType = typename decltype(segment_data_type)::type;
    const auto created_dict_segment = dynamic_pointer_cast<DictionarySegment<SegmentDataType>>(created_segment);
    auto dict_segment_iterable = create_iterable_from_segment<SegmentDataType>(*created_dict_segment);

    auto column_sum_of_created_chunk = SegmentDataType{};
    dict_segment_iterable.with_iterators([&](auto it, auto end) {
      column_sum_of_created_chunk = std::accumulate(it, end, SegmentDataType{0}, [](const auto& accumulator, const auto& currentValue) {
        return accumulator + SegmentDataType{currentValue.value()};
      });
    });

    std::cout << "Sum of column 6 of created chunk: " << column_sum_of_created_chunk << std::endl;

  });

  const auto mapped_segment = mapped_chunk->get_segment(ColumnID{5});

  resolve_data_type(mapped_segment->data_type(), [&](auto segment_data_type) {
    using SegmentDataType = typename decltype(segment_data_type)::type;
    const auto mapped_dict_segment = dynamic_pointer_cast<DictionarySegment<SegmentDataType>>(mapped_segment);
    auto mapped_dict_segment_iterable = create_iterable_from_segment<SegmentDataType>(*mapped_dict_segment);

    auto column_sum_of_mapped_chunk = SegmentDataType{};
    mapped_dict_segment_iterable.with_iterators([&](auto it, auto end) {
      column_sum_of_mapped_chunk = std::accumulate(it, end, SegmentDataType{0}, [](const auto& accumulator, const auto& currentValue) {
        return accumulator + SegmentDataType{currentValue.value()};
      });
    });

    std::cout << "Sum of column 6 of mapped chunk: " << column_sum_of_mapped_chunk << std::endl;

  });

  // print row 17 of created and mapped chunk
  std::cout << "Row 17 of created chunk: ";
  for (auto column_index = ColumnID{0}; column_index < 16; ++column_index) {
    const auto segment = chunk->get_segment(column_index);
    const auto attribute_value = (*segment)[ChunkOffset{16}];
    std::cout << attribute_value << " ";
  }
  std::cout << std::endl;


  std::cout << "Row 17 of mapped chunk: ";
  for (auto column_index = ColumnID{0}; column_index < 16; ++column_index) {
    const auto segment = mapped_chunk->get_segment(column_index);
    const auto attribute_value = (*segment)[ChunkOffset{16}];
    std::cout << attribute_value << " ";
  }
  std::cout << std::endl;
}

void StorageManager::replace_chunk_with_mmaped_chunk(const std::shared_ptr<Chunk>& chunk, ChunkID chunk_id, const std::string& table_name) {
  // get current persistence_file for table
  const auto table_persistence_file = get_persistence_file_name(table_name);
  // persist chunk to disk
  auto chunk_start_offset = persist_chunk_to_file(chunk, chunk_id, table_persistence_file);
  _tables_current_persistence_file_mapping[table_name].current_chunk_count++;

  // map chunk from disk
  const auto column_definitions = _tables[table_name]->column_data_types();
  auto mapped_chunk = map_chunk_from_disk(chunk_start_offset, table_persistence_file, chunk->column_count(), column_definitions);
  evaluate_mapped_chunk(chunk, mapped_chunk);

  // replace chunk in table
}

const std::string StorageManager::get_persistence_file_name(const std::string table_name){
  if (_tables_current_persistence_file_mapping[table_name].current_chunk_count == MAX_CHUNK_COUNT_PER_FILE) {
    const auto next_file_index = _tables_current_persistence_file_mapping[table_name].file_index + 1;
    auto next_persistence_file_name = table_name + "_" + std::to_string(next_file_index) + ".bin";
    _tables_current_persistence_file_mapping[table_name] = {next_persistence_file_name, next_file_index, 0};
  }
  return _tables_current_persistence_file_mapping[table_name].file_name;
}

FILE_HEADER StorageManager::read_file_header(const std::string& filename) {
  auto file_header = FILE_HEADER{};
  auto fd = int32_t{};

  Assert((fd = open(filename.c_str(), O_RDONLY)) >= 0, "Open error");
  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, _file_header_bytes, PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((map != MAP_FAILED), "Mapping Failed");
  close(fd);

  file_header.storage_format_version_id = map[0];
  file_header.chunk_count = map[1];

  const auto header_constants_size = element_index(_format_version_id_bytes + _chunk_count_bytes, 4);

  for (auto header_index = size_t{0}; header_index < file_header.chunk_count; ++header_index) {
    file_header.chunk_ids[header_index] = map[header_constants_size + header_index];
    file_header.chunk_offset_ends[header_index] =
        map[header_constants_size + StorageManager::_chunk_count + header_index];
  }
  munmap(map, _file_header_bytes);

  return file_header;
}

CHUNK_HEADER StorageManager::read_chunk_header(const std::string& filename, const uint32_t segment_count,
                                               const uint32_t chunk_offset_begin) {
  // TODO: Remove need to map the whole file.
  auto header = CHUNK_HEADER{};
  const auto map_index = element_index(chunk_offset_begin, 4);

  auto fd = int32_t{};
  Assert((fd = open(filename.c_str(), O_RDONLY)) >= 0, "Opening of file failed.");

  const auto file_bytes = std::filesystem::file_size(filename);
  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, file_bytes, PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((map != MAP_FAILED), "Mapping of Chunk Failed.");
  close(fd);

  header.row_count = map[map_index];

  for (auto segment_offset_index = size_t{0}; segment_offset_index < segment_count; ++segment_offset_index) {
    header.segment_offset_ends.emplace_back(map[segment_offset_index + map_index + 1]);
  }

  return header;
}

std::shared_ptr<Chunk> StorageManager::map_chunk_from_disk(const uint32_t chunk_offset_end, const std::string& filename,
                                                           const uint32_t segment_count, const std::vector<DataType> column_definitions) {
  auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};

  auto fd = int32_t{};
  Assert((fd = open(filename.c_str(), O_RDONLY)) >= 0, "Opening of file failed.");

  const auto file_bytes = std::filesystem::file_size(filename);

  // TODO: Remove unneccesary map on whole file
  const auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, file_bytes, PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((map != MAP_FAILED), "Mapping of File Failed.");
  close(fd);

  const auto chunk_header = read_chunk_header(filename, segment_count, chunk_offset_end);

  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    auto segment_offset_begin = _chunk_header_bytes(segment_count) + chunk_offset_end;

    if (segment_index > 0) {
      segment_offset_begin = chunk_header.segment_offset_ends[segment_index - 1] + chunk_offset_end;
    }

    resolve_data_type(column_definitions[segment_index], [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if constexpr(std::is_same<ColumnDataType, pmr_string>::value) {
        segments.emplace_back(std::make_shared<FixedStringDictionarySegment<ColumnDataType>>(map + segment_offset_begin / 4));
      } else {
        segments.emplace_back(std::make_shared<DictionarySegment<ColumnDataType>>(map + segment_offset_begin / 4));
      }
    });
  }


  const auto chunk = std::make_shared<Chunk>(segments);
  return chunk;
}

uint32_t StorageManager::_chunk_header_bytes(uint32_t column_count) {
  return _row_count_bytes + column_count * _segment_offset_bytes;
}

PersistedSegmentEncodingType StorageManager::resolve_persisted_segment_encoding_type_from_compression_type(CompressedVectorType compressed_vector_type) {
  PersistedSegmentEncodingType persisted_vector_type_id = {};
  switch (compressed_vector_type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
      persisted_vector_type_id = PersistedSegmentEncodingType::DictionaryEncoding32Bit;
      break;
    case CompressedVectorType::FixedWidthInteger2Byte:
      persisted_vector_type_id = PersistedSegmentEncodingType::DictionaryEncoding16Bit;
      break;
    case CompressedVectorType::FixedWidthInteger1Byte:
      persisted_vector_type_id = PersistedSegmentEncodingType::DictionaryEncoding8Bit;
      break;
    case CompressedVectorType::BitPacking:
      persisted_vector_type_id = PersistedSegmentEncodingType::DictionaryEncodingBitPacking;
      break;
    default:
      persisted_vector_type_id = PersistedSegmentEncodingType::Unencoded;
  }
  return persisted_vector_type_id;
}

}  // namespace hyrise
