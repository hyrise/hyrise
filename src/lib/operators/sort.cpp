#include "sort.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "boost/sort/pdqsort/pdqsort.hpp"
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/operator_performance_data.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/base_segment_accessor.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

#define SMALL_ARRAY_THRESHOLD 10'000

namespace {

using namespace hyrise;  // NOLINT

// Ceiling of integer division
size_t div_ceil(const size_t lhs, const ChunkOffset rhs) {
  DebugAssert(rhs > 0, "Divisor must be larger than 0.");
  return (lhs + rhs - 1u) / rhs;
}

bool is_descending(const SortMode& mode) {
  return mode == SortMode::DescendingNullsFirst || mode == SortMode::DescendingNullsLast;
}

bool is_nulls_first(const SortMode& mode) {
  return mode == SortMode::AscendingNullsFirst || mode == SortMode::DescendingNullsFirst;
}

inline void encode_string(uint8_t* dest, const size_t data_length, const pmr_string& value) {
  auto string_len = value.size();
  memset(dest + 1, 0, data_length);                                          // set all bytes to 0
  memcpy(dest + 1, value.data(), string_len);                                // copy the string data into the key buffer
  memset(dest + 1 + data_length - 2, static_cast<uint16_t>(string_len), 2);  // store actual string length
}

inline void encode_double(uint8_t* dest, const double value) {
  // Encode double value; reinterpret double as raw 64-bit bits
  auto bits = uint64_t{0};
  memcpy(&bits, &value, sizeof(bits));

  // Flip the bits to ensure lexicographic order matches numeric order
  if (std::signbit(value)) {
    bits = ~bits;  // Negative values are bitwise inverted
  } else {
    bits ^= 0x8000000000000000ULL;  // Flip the sign bit for positive values
  }

  // Write to buffer in big-endian order (MSB first)
  for (auto byte_idx = uint32_t{0}; byte_idx < 8; ++byte_idx) {
    dest[1 + byte_idx] = static_cast<uint8_t>(bits >> ((7 - byte_idx) * 8));
  }
}

inline void encode_float(uint8_t* dest, const float value) {
  auto bits = uint32_t{0};
  memcpy(&bits, &value, sizeof(bits));

  // Flip the bits to ensure lexicographic order matches numeric order
  if (std::signbit(value)) {
    bits = ~bits;  // Negative values are bitwise inverted
  } else {
    bits ^= 0x80000000;  // Flip the sign bit for positive values
  }

  // Write to buffer in big-endian order (MSB first)
  for (auto byte_idx = uint32_t{0}; byte_idx < 4; ++byte_idx) {
    dest[1 + byte_idx] = static_cast<uint8_t>(bits >> ((3 - byte_idx) * 8));
  }
}

template <typename T>
inline void encode_integer(uint8_t* dest, const T value, const size_t data_length) {
  // Bias the value to get a lexicographically sortable encoding
  using UnsignedT = typename std::make_unsigned<T>::type;
  UnsignedT biased = static_cast<UnsignedT>(value) ^ (UnsignedT(1) << (data_length * 8 - 1));  // flip sign bit

  // Store bytes in big-endian order starting at dest[1]
  for (auto byte_idx = size_t{0}; byte_idx < data_length; ++byte_idx) {
    s dest[1 + byte_idx] = static_cast<uint8_t>(biased >> ((data_length - 1 - byte_idx) * 8));
  }
}

template void encode_integer<int32_t>(uint8_t* dest, const int32_t value, const size_t data_length);
template void encode_integer<int64_t>(uint8_t* dest, const int64_t value, const size_t data_length);

template <typename Compare>
void parallel_sort_rowids(RowIDPosList& rows, Compare comp) {
  TRACE_EVENT("Sort", "ParallelSortRowIDs");
  auto row_count = rows.size();
  auto is_multithreaded = Hyrise::get().is_multi_threaded();

  if (!is_multithreaded || row_count < SMALL_ARRAY_THRESHOLD) {
    TRACE_EVENT("Sort", "ParallelSortRowIDs::SmallArray");
    std::sort(rows.begin(), rows.end(), comp);
    return;
  }

  // 1) get number of workers and block size
  auto scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler());
  if (!scheduler) {
    throw std::logic_error("Scheduler should be instance of NodeQueueScheduler.");
  }
  auto num_workers = scheduler->active_worker_count().load();
  size_t block = (row_count + num_workers - 1) / num_workers;

  // 2) sort each block in parallel
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(num_workers);
  for (auto t = int16_t{0}; t < num_workers; ++t) {
    size_t start = t * block;
    size_t end = std::min(start + block, row_count);
    if (start < end) {
      jobs.emplace_back(std::make_shared<JobTask>([start, end, &rows, &comp]() {
        TRACE_EVENT("Sort", "ParallelSortRowIDs::Sort");
        std::sort(rows.begin() + start, rows.begin() + end, comp);
      }));
    }
  }
  scheduler->schedule_and_wait_for_tasks(jobs);

  // 3) bottom-up merge sorted runs, doubling the run size each pass:
  size_t run = block;
  while (run < row_count) {
    jobs.clear();
    for (size_t left = 0; left + run < row_count; left += 2 * run) {
      size_t mid = left + run;
      size_t right = std::min(left + 2 * run, row_count);
      jobs.emplace_back(std::make_shared<JobTask>([left, mid, right, &rows, &comp]() {
        TRACE_EVENT("Sort", "ParallelSortRowIDs::Merge");
        std::inplace_merge(rows.begin() + left, rows.begin() + mid, rows.begin() + right, comp);
      }));
    }
    scheduler->schedule_and_wait_for_tasks(jobs);
    run *= 2;
  }
}

/**
 *        ____  _  _  __  ____  ____   __  ____   ___
 *       (    \( \/ )/  \(    \(___ \ /  \(___ \ / __)
 *        ) D ( )  /(  O )) D ( / __/(  0 )/ __/(___ \
 *       (____/(__/  \__/(____/(____) \__/(____)(____/
 *
 *
 * Notes on Segment Accessors:
 *   As discussed on June 30th, you do not need to use segment accessors. They can be handy, but for almost all cases,
 *   using segment_iterate (which will use SegmentAccessors in the background) will be the better option.
 */

// Given an unsorted_table and a pos_list that defines the output order, this materializes all columns in the table,
// creating chunks of output_chunk_size rows at maximum.
std::shared_ptr<Table> write_materialized_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                       RowIDPosList pos_list, const ChunkOffset output_chunk_size) {
  TRACE_EVENT("Sort", "WriteMaterializedOutputTable");
  // First, we create a new table as the output
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408
  auto output = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::Data, output_chunk_size);

  // After we created the output table and initialized the column structure, we can start adding values. Because the
  // values are not sorted by input chunks anymore, we can't process them chunk by chunk. Instead the values are copied
  // column by column for each output row.

  const auto output_chunk_count = div_ceil(pos_list.size(), output_chunk_size);
  Assert(pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList");

  // Materialize column by column, starting a new ValueSegment whenever output_chunk_size is reached
  const auto input_chunk_count = unsorted_table->chunk_count();
  const auto output_column_count = unsorted_table->column_count();
  const auto row_count = unsorted_table->row_count();

  // Vector of segments for each chunk
  auto output_segments_by_chunk = std::vector<Segments>(output_chunk_count, Segments(output_column_count));

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(output_chunk_count * output_column_count);

  for (auto column_id = ColumnID{0}; column_id < output_column_count; ++column_id) {
    const auto column_data_type = output->column_data_type(column_id);
    const auto column_is_nullable = unsorted_table->column_is_nullable(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto accessor_by_chunk_id =
          std::vector<std::shared_ptr<AbstractSegmentAccessor<ColumnDataType>>>(unsorted_table->chunk_count());
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto& abstract_segment = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
        accessor_by_chunk_id[input_chunk_id] = create_segment_accessor<ColumnDataType>(abstract_segment);
      }

      for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
        jobs.emplace_back(std::make_shared<JobTask>([&, output_chunk_id, column_id,
                                                     column_is_nullable = column_is_nullable,
                                                     accessor_by_chunk_id = accessor_by_chunk_id]() {
          TRACE_EVENT("Sort", "WriteMaterializedOutputTable::Column::Chunk", "ColumnID", static_cast<size_t>(column_id),
                      "ChunkID", static_cast<size_t>(output_chunk_id));

          auto value_segment_value_vector = pmr_vector<ColumnDataType>{};
          auto value_segment_null_vector = pmr_vector<bool>{};

          const auto chunk_size =
              std::min(output_chunk_size, static_cast<ChunkOffset>(row_count - output_chunk_id * output_chunk_size));

          value_segment_value_vector.reserve(chunk_size);
          if (column_is_nullable) {
            value_segment_null_vector.reserve(chunk_size);
          }

          for (auto row_index = size_t{0}; row_index < chunk_size; ++row_index) {
            const auto [chunk_id, chunk_offset] = pos_list[output_chunk_size * output_chunk_id + row_index];

            auto& accessor = accessor_by_chunk_id[chunk_id];
            const auto typed_value = accessor->access(chunk_offset);
            const auto is_null = !typed_value;
            value_segment_value_vector.push_back(is_null ? ColumnDataType{} : typed_value.value());
            if (column_is_nullable) {
              value_segment_null_vector.push_back(is_null);
            }
          }

          std::shared_ptr<ValueSegment<ColumnDataType>> value_segment;
          if (column_is_nullable) {
            value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                           std::move(value_segment_null_vector));
          } else {
            value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector));
          }

          output_segments_by_chunk[output_chunk_id][column_id] = value_segment;
        }));
        jobs.back()->schedule();  // schedule immediately because job creation is somewhat expensive
      }
    });
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);

  for (auto& segments : output_segments_by_chunk) {
    output->append_chunk(segments);
  }

  return output;
}

// Given an unsorted_table and an input_pos_list that defines the output order, this writes the output table as a
// reference table. This is usually faster, but can only be done if a single column in the input table does not
// reference multiple tables. An example where this restriction applies is the sorted result of a union between two
// tables. The restriction is needed because a ReferenceSegment can only reference a single table. It does, however,
// not necessarily apply to joined tables, so two tables referenced in different columns is fine.
//
// If unsorted_table is of TableType::Data, this is trivial and the input_pos_list is used to create the output
// reference table. If the input is already a reference table, the double indirection needs to be resolved.
std::shared_ptr<Table> write_reference_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                    RowIDPosList input_pos_list, const ChunkOffset output_chunk_size) {
  TRACE_EVENT("Sort", "WriteReferenceOutputTable");
  // First we create a new table as the output
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408
  auto output_table = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::References);

  const auto resolve_indirection = unsorted_table->type() == TableType::References;
  const auto column_count = output_table->column_count();

  const auto output_chunk_count = div_ceil(input_pos_list.size(), output_chunk_size);
  const auto row_count = unsorted_table->row_count();
  Assert(input_pos_list.size() == row_count, "Mismatching size of input table and PosList");

  // Vector of segments for each chunk
  auto output_segments_by_chunk = std::vector<Segments>(output_chunk_count, Segments(column_count));

  if (!resolve_indirection && input_pos_list.size() <= output_chunk_size) {
    // Shortcut: No need to copy RowIDs if input_pos_list is small enough and we do not need to resolve the indirection.
    const auto output_pos_list = std::make_shared<RowIDPosList>(std::move(input_pos_list));
    auto& output_segments = output_segments_by_chunk.at(0);
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      output_segments[column_id] = std::make_shared<ReferenceSegment>(unsorted_table, column_id, output_pos_list);
    }
  } else {
    const auto input_chunk_count = unsorted_table->chunk_count();

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(static_cast<size_t>(column_count));

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      // Collect all input segments for the current column
      auto input_segments = std::vector<std::shared_ptr<AbstractSegment>>(input_chunk_count);
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        input_segments[input_chunk_id] = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
      }

      const auto first_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(input_segments.at(0));
      const auto referenced_table = resolve_indirection ? first_reference_segment->referenced_table() : unsorted_table;
      const auto referenced_column_id =
          resolve_indirection ? first_reference_segment->referenced_column_id() : column_id;

      for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
        jobs.emplace_back(std::make_shared<JobTask>([&, output_chunk_id, column_id, input_segments = input_segments,
                                                     referenced_table = referenced_table,
                                                     referenced_column_id = referenced_column_id]() {
          TRACE_EVENT("Sort", "WriteReferenceOutputTable::Column::Chunk", "ColumnID", static_cast<size_t>(column_id),
                      "ChunkID", static_cast<size_t>(output_chunk_id));

          const auto chunk_size =
              std::min(output_chunk_size, static_cast<ChunkOffset>(row_count - output_chunk_id * output_chunk_size));

          // To keep the implementation simple, we write the output ReferenceSegments column by column.
          // This means that even if input ReferenceSegments share a PosList,
          //  the output will contain independent PosLists. While this is
          // slightly more expensive to generate and slightly less efficient for following operators,
          // we assume that the lion's share of the work has been done
          // before the Sort operator is executed and that the relative cost of this
          // is acceptable. In the future, this could be improved.
          auto output_pos_list = std::make_shared<RowIDPosList>();
          output_pos_list->reserve(chunk_size);

          for (auto row_index = size_t{0}; row_index < chunk_size; ++row_index) {
            const auto& row_id = input_pos_list[output_chunk_size * output_chunk_id + row_index];

            if (resolve_indirection) {
              const auto& input_reference_segment = static_cast<ReferenceSegment&>(*input_segments[row_id.chunk_id]);
              DebugAssert(input_reference_segment.referenced_table() == referenced_table,
                          "Input column references more than one table");
              DebugAssert(input_reference_segment.referenced_column_id() == referenced_column_id,
                          "Input column references more than one column");
              const auto& input_reference_pos_list = input_reference_segment.pos_list();
              output_pos_list->emplace_back((*input_reference_pos_list)[row_id.chunk_offset]);
            } else {
              output_pos_list->emplace_back(row_id);
            }
          }

          DebugAssert(!output_pos_list->empty(), "Asked to write empty output_pos_list");
          output_segments_by_chunk.at(output_chunk_id)[column_id] =
              std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, output_pos_list);
        }));
        jobs.back()->schedule();  // schedule job immediately
      }
    }

    Hyrise::get().scheduler()->wait_for_tasks(jobs);
  }

  for (auto& segments : output_segments_by_chunk) {
    output_table->append_chunk(segments);
  }

  return output_table;
}

}  // namespace

namespace hyrise {

Sort::Sort(const std::shared_ptr<const AbstractOperator>& input_operator,
           const std::vector<SortColumnDefinition>& sort_definitions, const ChunkOffset output_chunk_size,
           const ForceMaterialization force_materialization)
    : AbstractReadOnlyOperator(OperatorType::Sort, input_operator, nullptr,
                               std::make_unique<OperatorPerformanceData<OperatorSteps>>()),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size),
      _force_materialization(force_materialization) {
  DebugAssert(!_sort_definitions.empty(), "Expected at least one sort criterion");
}

const std::vector<SortColumnDefinition>& Sort::sort_definitions() const {
  return _sort_definitions;
}

const std::string& Sort::name() const {
  static const auto name = std::string{"Sort"};
  return name;
}

std::shared_ptr<AbstractOperator> Sort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Sort>(copied_left_input, _sort_definitions, _output_chunk_size, _force_materialization);
}

void Sort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Sort::_on_execute() {
  TRACE_EVENT("Sort", "Execute");
  auto timer = Timer{};
  const auto& input_table = left_input_table();

  // validate sort definitions
  for (const auto& column_sort_definition : _sort_definitions) {
    Assert(column_sort_definition.column != INVALID_COLUMN_ID, "Sort: Invalid column in sort definition");
    Assert(column_sort_definition.column < input_table->column_count(),
           "Sort: Column ID is greater than table's column count");
  }

  // edge case: empty input table
  if (input_table->row_count() == 0) {
    if (_force_materialization == ForceMaterialization::Yes && input_table->type() == TableType::References) {
      return Table::create_dummy_table(input_table->column_definitions());
    }

    return input_table;
  }

  // After the first (least significant) sort operation has been completed, this holds the order of the table as it has
  // been determined so far. This is not a completely proper PosList on the input table as it might point to
  // ReferenceSegments.
  auto sorted_table = std::shared_ptr<Table>{};

  const auto chunk_count = input_table->chunk_count();
  const auto row_count = input_table->row_count();
  const auto sort_definitions_size = _sort_definitions.size();

  /**************************************************************************************************************
   ***************************************** Pre-compute offsets and other info *********************************
   **************************************************************************************************************/

  // based on the sizes of the columns to be sorted by, e.g. if sorting by int, string it should be [4, 8]
  auto field_width = std::vector<size_t>();
  field_width.reserve(sort_definitions_size);

  TRACE_EVENT_BEGIN("Sort", "Execute::PrecomputeFieldWidth");
  for (auto index = size_t{0}; index < sort_definitions_size; ++index) {
    const auto& def = _sort_definitions[index];
    const auto sort_col = def.column;
    resolve_data_type(input_table->column_data_type(sort_col), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        // iterate over all chunks to find the longest string in the column
        auto max_string_length = size_t{0};
        for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto abstract_segment = input_table->get_chunk(chunk_id)->get_segment(sort_col);
          segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& val) {
            if (!val.is_null()) {
              const auto string_length = val.value().size();
              if (string_length > max_string_length) {
                max_string_length = string_length;
              }
            }
          });
        }

        field_width.emplace_back(max_string_length + 2);  // store size of the string prefix + 2 for string length
      } else {
        field_width.push_back(
            sizeof(ColumnDataType));  // store size of the column type, e.g. 4 for int, 8 for double, etc.
      }
    });
  }
  TRACE_EVENT_END("Sort");

  // total width of each normalized key (width of all columns to be sorted by plus null bytes)
  auto key_width = size_t{0};
  for (const auto& column : field_width) {
    key_width += column + 1;  // +1 for null byte
  }

  /**
   * Offsets for each column in the key, i.e. `key_offsets[i]` is the offset of the i-th column in the key.
   * This means, that `buffer[key_offsets[i]]` is the location of the i-th column's value in the key.
  */
  auto key_offsets = std::vector<size_t>(sort_definitions_size);
  key_offsets[0] = 0;  // first column starts at offset 0
  for (auto index = size_t{1}; index < sort_definitions_size; ++index) {
    key_offsets[index] = key_offsets[index - 1] + field_width[index - 1] + 1;  // +1 for null byte
  }

  TRACE_EVENT_BEGIN("Sort", "Execute::PrecomputeChunkSizes");
  auto key_buffer = std::vector<uint8_t>();  // buffer to hold all keys for sorting
  auto total_buffer_size = size_t{0};        // total size of the key buffer

  // number of rows per chunk in the input table
  auto chunk_sizes = std::vector<size_t>(chunk_count);

  auto row_ids = RowIDPosList{};  // vector to hold row IDs for each row in the table
  row_ids.reserve(row_count);     // reserve space for row IDs

  auto row_id_offsets = std::vector<size_t>();  // offsets for each chunk's row IDs
  row_id_offsets.reserve(chunk_count);          // reserve space for offsets
  row_id_offsets.emplace_back(0);               // first chunk starts at offset 0

  for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = input_table->get_chunk(chunk_id);
    auto row_count_for_chunk = chunk->size();

    chunk_sizes[chunk_id] = row_count_for_chunk;

    total_buffer_size += row_count_for_chunk * key_width;  // total size of the keys for all chunks

    // offset for the next chunk
    if (chunk_id > 0) {
      row_id_offsets.emplace_back(row_id_offsets[chunk_id - 1] + chunk_sizes[chunk_id - 1]);
    }

    for (ChunkOffset row = ChunkOffset{0}; row < row_count_for_chunk; ++row) {
      row_ids.emplace_back(chunk_id, row);  // build array of row ids for this chunk
    }
  }
  TRACE_EVENT_END("Sort");

  key_buffer.reserve(total_buffer_size);  // reserve space for all keys
  auto preparation_time = timer.lap();

  /**************************************************************************************************************
   *********************************************** Key-Generation ***********************************************
   **************************************************************************************************************/

  // Custom comparator function used for sorting
  auto compare_rows = [&](const RowID a, const RowID b) {
    auto* key_a = &key_buffer[(row_id_offsets[a.chunk_id] + a.chunk_offset) * key_width];
    auto* key_b = &key_buffer[(row_id_offsets[b.chunk_id] + b.chunk_offset) * key_width];

    return memcmp(key_a, key_b, key_width) < 0;
  };

  TRACE_EVENT_BEGIN("Sort", "Execute::GenerateKeys");
  // job queue for generating keys in parallel
  auto keygen_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  keygen_jobs.reserve(static_cast<size_t>(chunk_count) * sort_definitions_size);
  // for each chunk in table
  for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = input_table->get_chunk(chunk_id);

    // TODO(someone): How to handle huge number of keycolumns? maybe max. number for key generation
    for (auto index = size_t{0}; index < sort_definitions_size; ++index) {
      keygen_jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id, index, chunk = chunk]() {
        TRACE_EVENT("Sort", "Execute::GenerateKeys", "ChunkID", static_cast<size_t>(chunk_id), "ColumnID",
                    static_cast<size_t>(_sort_definitions[index].column));

        auto sort_col = _sort_definitions[index].column;
        auto nulls_first = is_nulls_first(_sort_definitions[index].sort_mode);
        auto descending = is_descending(_sort_definitions[index].sort_mode);

        const auto abstract_segment = chunk->get_segment(sort_col);
        resolve_data_type(input_table->column_data_type(sort_col), [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& val) {
            const auto row = val.chunk_offset();  // get the row offset in the chunk

            // pointer to the destination in the key buffer for this column
            auto* dest = &key_buffer[(row_id_offsets[chunk_id] + row) * key_width + key_offsets[index]];

            const ColumnDataType value = val.value();
            const auto data_length = field_width[index];

            // Set the first byte to indicate if the value is null or not
            auto null_byte = !val.is_null() ? 0x00 : 0xFF;
            if (nulls_first) {
              null_byte = ~null_byte;
            }
            dest[0] = static_cast<uint8_t>(null_byte);

            // encode the value into the key based on the data type
            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
              encode_string(dest, data_length, value);
            } else if constexpr (std::is_same_v<ColumnDataType, double>) {
              encode_double(dest, value);
            } else if constexpr (std::is_same_v<ColumnDataType, float>) {
              encode_float(dest, value);
            } else if constexpr (std::is_integral<ColumnDataType>::value && std::is_signed<ColumnDataType>::value) {
              encode_integer<ColumnDataType>(dest, value, data_length);
            } else {
              throw std::logic_error("Unsupported data type for sorting: " +
                                     std::string(typeid(ColumnDataType).name()));
            }

            // Invert for descending order (excluding the null byte)
            if (descending) {
              for (auto idx = size_t{1}; idx <= data_length; ++idx) {
                dest[idx] = ~dest[idx];
              }
            }
          });
        });
      }));
    }
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(keygen_jobs);  // wait for all chunks to be materialized
  TRACE_EVENT_END("Sort");
  auto key_generation_time = timer.lap();

  /**************************************************************************************************************
   ************************************************** Mergesort *************************************************
   **************************************************************************************************************/

  parallel_sort_rowids(row_ids, compare_rows);
  auto merge_sort_time = timer.lap();

  /**************************************************************************************************************
   *********************************************** Output writing ***********************************************
   **************************************************************************************************************/

  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  step_performance_data.set_step_runtime(OperatorSteps::Preparation, preparation_time);
  step_performance_data.set_step_runtime(OperatorSteps::MaterializeSortColumns, key_generation_time);
  step_performance_data.set_step_runtime(OperatorSteps::Sort, merge_sort_time);

  // We have to materialize the output (i.e., write ValueSegments) if
  //  (a) it is requested by the user,
  //  (b) a column in the table references multiple tables (see write_reference_output_table for details), or
  //  (c) a column in the table references multiple columns in the same table (which is an unlikely edge case).
  // Cases (b) and (c) can only occur if there is more than one ReferenceSegment in an input chunk.
  auto must_materialize = _force_materialization == ForceMaterialization::Yes;
  const auto input_chunk_count = input_table->chunk_count();
  if (!must_materialize && input_table->type() == TableType::References && input_chunk_count > 1) {
    const auto input_column_count = input_table->column_count();

    for (auto input_column_id = ColumnID{0}; input_column_id < input_column_count; ++input_column_id) {
      const auto& first_segment = input_table->get_chunk(ChunkID{0})->get_segment(input_column_id);
      const auto& first_reference_segment = static_cast<ReferenceSegment&>(*first_segment);

      const auto& common_referenced_table = first_reference_segment.referenced_table();
      const auto& common_referenced_column_id = first_reference_segment.referenced_column_id();

      for (auto input_chunk_id = ChunkID{1}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto& segment = input_table->get_chunk(input_chunk_id)->get_segment(input_column_id);
        const auto& referenced_table = static_cast<ReferenceSegment&>(*segment).referenced_table();
        const auto& referenced_column_id = static_cast<ReferenceSegment&>(*segment).referenced_column_id();

        if (common_referenced_table != referenced_table || common_referenced_column_id != referenced_column_id) {
          must_materialize = true;
          break;
        }
      }
      if (must_materialize) {
        break;
      }
    }
  }

  if (must_materialize) {
    sorted_table = write_materialized_output_table(input_table, std::move(row_ids), _output_chunk_size);
  } else {
    sorted_table = write_reference_output_table(input_table, std::move(row_ids), _output_chunk_size);
  }

  TRACE_EVENT("Sort", "Execute::SetIndividuallySortedBy");
  const auto& final_sort_definition = _sort_definitions[0];
  // Set the sorted_by attribute of the output's chunks according to the most significant sort operation, which is the
  // column the table was sorted by last.
  const auto output_chunk_count = sorted_table->chunk_count();
  auto write_output_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  write_output_jobs.reserve(static_cast<size_t>(output_chunk_count));
  for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
    write_output_jobs.emplace_back(std::make_shared<JobTask>([&, output_chunk_id] {
      TRACE_EVENT("Sort", "Execute::SetIndividuallySortedBy::Chunk", "ChunkID", static_cast<size_t>(output_chunk_id));
      const auto& output_chunk = sorted_table->get_chunk(output_chunk_id);
      output_chunk->set_immutable();
      output_chunk->set_individually_sorted_by(final_sort_definition);
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(write_output_jobs);

  auto write_output_time = timer.lap();
  step_performance_data.set_step_runtime(OperatorSteps::WriteOutput, write_output_time);

  return sorted_table;
}

}  // namespace hyrise
