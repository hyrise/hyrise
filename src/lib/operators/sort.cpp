#include "sort.hpp"

#include <iostream>

#ifdef ENABLE_PERFETTO
#include <perfetto.h>
#else
#define PERFETTO_DEFINE_CATEGORIES(...)
#define PERFETTO_TRACK_EVENT_STATIC_STORAGE(...)
#define TRACE_EVENT(...)
#define TRACE_EVENT_BEGIN(...)
#define TRACE_EVENT_END(...)
#endif

#include <algorithm>
#include <bit>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <execution>
#include <format>
#include <fstream>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/accumulators/statistics_fwd.hpp>
#include <boost/algorithm/cxx11/iota.hpp>
#include <boost/range/numeric.hpp>
#include <boost/sort/pdqsort/pdqsort.hpp>

#include "all_type_variant.hpp"
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/operator_performance_data.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/base_segment_accessor.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/segment_iterables/segment_positions.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

PERFETTO_DEFINE_CATEGORIES(perfetto::Category("sort").SetDescription("Benchmark Sort Operator"));

PERFETTO_TRACK_EVENT_STATIC_STORAGE();

namespace {

using namespace hyrise;  // NOLINT

constexpr size_t RADIX = 256;
constexpr size_t INSERTION_SORT_THRESHOLD = 32;
constexpr size_t MSD_RADIX_SORT_SIZE_THRESHOLD = 4;

// constexpr size_t PARALLEL_THRESHOLD = 10000;

// Ceiling of integer division
size_t div_ceil(const size_t lhs, const ChunkOffset rhs) {
  DebugAssert(rhs > 0, "Divisor must be larger than 0.");
  return (lhs + rhs - 1u) / rhs;
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

size_t opt_batch_size(size_t count, size_t min_batch_size, size_t max_tasks_per_thread) {
  const auto hardware_concurrency = std::thread::hardware_concurrency();
  const auto max_count_per_thread = (count + hardware_concurrency - 1) / hardware_concurrency;
  const auto max_count_per_batch = (max_count_per_thread + max_tasks_per_thread - 1) / max_tasks_per_thread;
  return std::max(max_count_per_batch, min_batch_size);
}

std::vector<std::shared_ptr<AbstractTask>> run_parallel_batched(size_t count, size_t batch_size, auto handler) {
  TRACE_EVENT("sort", "run_parallel_batched");

  const auto task_count = (count + batch_size - 1) / batch_size;
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>();
  tasks.reserve(task_count);

  for (auto task_index = size_t{0}; task_index < task_count; ++task_index) {
    TRACE_EVENT("sort", "setup task", "task", task_index);
    const auto begin = task_index * batch_size;
    const auto end = std::min(begin + batch_size, count);
    const auto task = std::make_shared<JobTask>([begin, end, handler] {
      TRACE_EVENT("sort", "task", "begin", begin, "end", end);
      for (auto index = begin; index < end; ++index) {
        handler(index);
      }
    });
    task->schedule();
    tasks.push_back(task);
  }
  return tasks;
}

// Given an unsorted_table and a pos_list that defines the output order, this materializes all columns in the table,
// creating chunks of output_chunk_size rows at maximum.
std::shared_ptr<Table> write_materialized_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                       RowIDPosList pos_list, const ChunkOffset output_chunk_size) {
  TRACE_EVENT("sort", "write_materialized_output_table");

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
  auto output_segments_by_chunk = std::vector(output_chunk_count, Segments(output_column_count));

  const auto materialize_columns = run_parallel_batched(output_column_count, 1, [&](const auto column_index) {
    TRACE_EVENT("sort", "materialize_column", "column_id", column_index);
    const auto column_id = ColumnID{static_cast<uint16_t>(column_index)};

    const auto column_data_type = output->column_data_type(column_id);
    const auto column_is_nullable = unsorted_table->column_is_nullable(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto accessor_by_chunk_id =
          std::vector<std::unique_ptr<AbstractSegmentAccessor<ColumnDataType>>>(input_chunk_count);
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto& abstract_segment = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
        DebugAssert(abstract_segment, "Segment should exist");
        accessor_by_chunk_id[input_chunk_id] = create_segment_accessor<ColumnDataType>(abstract_segment);
        DebugAssert(accessor_by_chunk_id[input_chunk_id], "Accessor must be initialized");
      }

      const auto materialize_chunks = run_parallel_batched(output_chunk_count, 8, [&](const auto chunk_index) {
        TRACE_EVENT_BEGIN("sort", "materialize_chunk", "column_id", column_index, "chunk_id", chunk_index);
        const auto output_chunk_id = ChunkID{static_cast<uint32_t>(chunk_index)};

        const auto output_segment_begin = chunk_index * output_chunk_size;
        const auto output_segment_end = std::min(output_segment_begin + output_chunk_size, row_count);
        const auto output_segment_size = output_segment_end - output_segment_begin;

        auto value_segment_values = pmr_vector<ColumnDataType>();
        value_segment_values.reserve(output_segment_size);
        auto value_segment_null_values = pmr_vector<bool>();
        if (column_is_nullable) {
          value_segment_null_values.reserve(output_segment_size);
        }

        const auto pos_list_begin = pos_list.begin() + output_segment_begin;
        const auto pos_list_end = pos_list.begin() + output_segment_end;
        for (const auto [input_chunk_id, input_chunk_offset] : std::ranges::subrange(pos_list_begin, pos_list_end)) {
          DebugAssert(accessor_by_chunk_id[input_chunk_id], "ChunkID must be valid");
          auto value = accessor_by_chunk_id[input_chunk_id]->access(input_chunk_offset);
          value_segment_null_values.push_back(!value);
          value_segment_values.push_back((value) ? *value : ColumnDataType{});
        }

        if (column_is_nullable) {
          const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(
              std::move(value_segment_values), std::move(value_segment_null_values));
          output_segments_by_chunk[output_chunk_id][column_id] = value_segment;
        } else {
          const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_values));
          output_segments_by_chunk[output_chunk_id][column_id] = value_segment;
        }
        TRACE_EVENT_END("sort");
      });
      Hyrise::get().scheduler()->wait_for_tasks(materialize_chunks);
    });
  });
  Hyrise::get().scheduler()->wait_for_tasks(materialize_columns);

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
  TRACE_EVENT("sort", "write_reference_output_table");
  // First we create a new table as the output
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408
  auto output_table = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::References);

  const auto resolve_indirection = unsorted_table->type() == TableType::References;
  const auto column_count = output_table->column_count();

  const auto output_chunk_count = div_ceil(input_pos_list.size(), output_chunk_size);
  Assert(input_pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList");

  // Vector of segments for each chunk
  auto output_segments_by_chunk = std::vector<Segments>(output_chunk_count, Segments(column_count));

  if (!resolve_indirection && input_pos_list.size() <= output_chunk_size) {
    TRACE_EVENT("sort", "write_fast_path");
    // Shortcut: No need to copy RowIDs if input_pos_list is small enough and we do not need to resolve the indirection.
    const auto output_pos_list = std::make_shared<RowIDPosList>(std::move(input_pos_list));
    auto& output_segments = output_segments_by_chunk.at(0);
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      output_segments[column_id] = std::make_shared<ReferenceSegment>(unsorted_table, column_id, output_pos_list);
    }
  } else {
    TRACE_EVENT("sort", "write_slow_path");

    TRACE_EVENT_BEGIN("sort", "write_setup");
    // Collect all segments from the input table.
    const auto input_chunk_count = unsorted_table->chunk_count();
    auto input_segments = std::vector(column_count, std::vector<std::shared_ptr<AbstractSegment>>(input_chunk_count));
    for (auto chunk_id = ChunkID{0}; chunk_id < input_chunk_count; ++chunk_id) {
      const auto chunk = unsorted_table->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        input_segments[column_id][chunk_id] = chunk->get_segment(column_id);
        DebugAssert(input_segments[column_id][chunk_id], "Expected valid segment");
      }
    }
    TRACE_EVENT_END("sort");

    // To keep the implementation simple, we write the output ReferenceSegments in column and chunk pairs. This means
    // that even if input ReferenceSegments share a PosList, the output will contain independent PosLists. While this
    // is slightly more expensive to generate and slightly less efficient for following operators, we assume that the
    // lion's share of the work has been done before the Sort operator is executed and that the relative cost of this
    // is acceptable. In the future, this could be improved.
    auto column_write_tasks = std::vector(column_count, std::vector<std::shared_ptr<AbstractTask>>());
    const auto batch_size = opt_batch_size(output_chunk_count, 8, (8 + column_count - 1) / column_count);
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      column_write_tasks[column_id] = run_parallel_batched(
          output_chunk_count, batch_size,
          [column_id, output_chunk_size, &input_pos_list, &input_segments, &unsorted_table, &output_segments_by_chunk,
           resolve_indirection](const size_t output_chunk_index) {
            TRACE_EVENT_BEGIN("sort", "write_segment", "column_id", static_cast<size_t>(column_id), "chunk_id",
                              output_chunk_index);

            auto input_pos_list_offset = size_t{output_chunk_index * output_chunk_size};
            auto input_pos_list_end = std::min(input_pos_list_offset + output_chunk_size, input_pos_list.size());

            const auto first_reference_segment =
                std::dynamic_pointer_cast<ReferenceSegment>(input_segments[column_id][output_chunk_index]);
            const auto referenced_table =
                resolve_indirection ? first_reference_segment->referenced_table() : unsorted_table;
            const auto referenced_column_id = resolve_indirection ? first_reference_segment->referenced_column_id()
                                                                  : ColumnID{static_cast<uint16_t>(column_id)};

            auto output_pos_list = std::make_shared<RowIDPosList>();
            output_pos_list->reserve(output_chunk_size);

            // Iterate over rows in sorted input pos list, dereference them if necessary, and write a chunk every
            // `output_chunk_size` rows.
            for (; input_pos_list_offset < input_pos_list_end; ++input_pos_list_offset) {
              const auto& row_id = input_pos_list[input_pos_list_offset];
              if (resolve_indirection) {
                const auto& input_reference_segment =
                    static_cast<ReferenceSegment&>(*input_segments[column_id][row_id.chunk_id]);
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

            output_segments_by_chunk[output_chunk_index][column_id] =
                std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, output_pos_list);

            TRACE_EVENT_END("sort");
          });
    }

    for (const auto& write_tasks : column_write_tasks) {
      Hyrise::get().scheduler()->wait_for_tasks(write_tasks);
    }
  }

  for (auto& segments : output_segments_by_chunk) {
    output_table->append_chunk(segments);
  }

  return output_table;
}

template <size_t start, size_t end>
int static_memcmp(std::byte* left, std::byte* right, size_t len) {
  if (len == start) {
    return memcmp(left, right, start);
  }
  if constexpr (start < end) {
    return static_memcmp<start + 1, end>(left, right, len);
  } else {
    return memcmp(left, right, len);
  }
}

struct NormalizedKeyRow {
  std::byte* key_head;
  RowID row_id;

  bool less_than(const NormalizedKeyRow& other, size_t expected_size) const {
    if (expected_size == 0) {
      return false;
    }
    return static_memcmp<1, 32>(key_head, other.key_head, expected_size) < 0;
  }
};

// Map signed to unsigned data types with same number of bytes (e.g., int32_t to uint32_t).
template <typename T>
auto to_unsigned(T value) {
  if constexpr (std::is_same_v<decltype(value), int32_t> || std::is_same_v<decltype(value), float>) {
    return std::bit_cast<uint32_t>(value);
  } else if constexpr (std::is_same_v<decltype(value), int64_t> || std::is_same_v<decltype(value), double>) {
    return std::bit_cast<uint64_t>(value);
  }
}

struct ScanResult {
  // Maximum number of bytes required to encode all values.
  size_t encoding_width = 0;
  // Number of extra bytes required to encode the length of an string.
  size_t extra_width = 0;
  // Contains a null value.
  bool nullable = false;

  // Returns number of bytes required to encode all scanned values. Includes the extra byte for null values.
  size_t width() const {
    return encoding_width + extra_width + ((nullable) ? 1 : 0);
  }

  ScanResult merge(ScanResult other) {
    return {
        .encoding_width = std::max(encoding_width, other.encoding_width),
        .extra_width = std::max(extra_width, other.extra_width),
        .nullable = nullable || other.nullable,
    };
  }
};

/// Scans column for maximum bytes necessary to encode all segmenet values and null values.
template <typename ColumnDataType>
ScanResult scan_column(const AbstractSegment& segment) {
  auto result = ScanResult{
      .encoding_width = 0,
      .extra_width = 0,
      .nullable = false,
  };
  segment_with_iterators<ColumnDataType>(segment, [&](auto it, const auto end) {
    while (it != end) {
      const auto& segment_position = *it;
      if (segment_position.is_null()) {
        result.nullable = true;
      } else if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        result.encoding_width = std::max(result.encoding_width, segment_position.value().size());
        const auto extra_width = static_cast<size_t>((std::countl_zero(result.encoding_width) + 7) / 8);
        result.extra_width = std::max(result.extra_width, extra_width);
      } else {
        const auto& value = segment_position.value();
        if constexpr (std::is_same_v<ColumnDataType, int32_t> || std::is_same_v<ColumnDataType, int64_t>) {
          using UnsignedColumnDataType = decltype(to_unsigned(ColumnDataType{}));
          static_assert(std::is_unsigned_v<UnsignedColumnDataType>, "Can not convert column data type to unsigned");
          // We often encounter small number, but we still 32-bit numbers while 8 or 16 bit would be sufficient.
          // Because of that we want to reduce the bit-width of our integer numbers to the bare minimum. The idea
          // is to drop all leading 0 for positive integers and all 1 leading 1 for negative numbers. We only keep
          // 1-bit to store the signess for our integers. To keep the reduction simple we only drop full bytes.
          //
          // Example for 32-bit:
          //
          // Number | Hex Representation | Number of dropped bytes | Shortened Int
          // -------|--------------------|-------------------------|--------------
          //   -42  | 0xFFFFFFd6         | 6                       | 0xd6
          //    42  | 0x0000002a         | 6                       | 0x2a
          constexpr auto BASE_BYTE_COUNT = sizeof(UnsignedColumnDataType);
          if (value < 0) {
            result.encoding_width = std::max(
                result.encoding_width, BASE_BYTE_COUNT - (std::countl_one<UnsignedColumnDataType>(value) - 1) / 8);
          } else {
            result.encoding_width = std::max(
                result.encoding_width, BASE_BYTE_COUNT - (std::countl_zero<UnsignedColumnDataType>(value) - 1) / 8);
          }
        } else if constexpr (std::is_same_v<ColumnDataType, float>) {
          result.encoding_width = 4;
        } else if constexpr (std::is_same_v<ColumnDataType, double>) {
          result.encoding_width = 8;
        }
      }
      ++it;
    }
  });
  return result;
}

// Swap byte order. Should be replaced with C++23's std::byteswap if possible.
auto byteswap(std::unsigned_integral auto value) {
  auto* uint_bytes = reinterpret_cast<std::byte*>(&value);
  for (auto start = uint_bytes, end = uint_bytes + sizeof(decltype(value)) - 1; start < end; ++start, --end) {
    std::swap(*start, *end);
  }
  return value;
}

// Copy an integer to the byte array. To ensure the encoded integer is comparable with memcmp the endianess of the
// encoded integer is changed to big-endian. In addition, it will take the number of bytes necessary to encode this
// integer, because of the variable length encoding of int32_t and uint32_t (see materialization and scanning).
void copy_uint_to_byte_array(std::byte* byte_array, std::unsigned_integral auto uint,
                             size_t len = sizeof(decltype(uint))) {
  if constexpr (std::endian::native == std::endian::little) {
    uint = byteswap(uint);
  } else if constexpr (std::endian::native != std::endian::big) {
    Fail("mixed-endian is unsupported");
  }
  const auto* uint_byte_array = reinterpret_cast<std::byte*>(&uint);
  memcpy(byte_array, uint_byte_array + (sizeof(decltype(uint)) - len), len);
}

using NormalizedKeyIter = pmr_vector<NormalizedKeyRow>::iterator;

// Append segement's values as byte array to the normalized keys. Expects that the normalized_key_iter is valid for
// each segment's values.
//
// Parameters:
// @param segment              Segment to encode values from
// @param offset               Offset to the start of the normalized key row's byte array.
// @param expected_width       The number of bytes all values must be encoded to.
// @param ascending            Sort normalized keys in ascending order.
// @param nullable             Put exrta byte in front to encode null values.
// @param nulls_first          Put null values to the start of the normalized key order else to the back.
// @param normalized_key_iter  Iterator to the normalized keys.
template <typename ColumnDataType>
void materialize_segment_as_normalized_keys(const AbstractSegment& segment, const size_t offset, const bool ascending,
                                            const ScanResult column_info, const bool nulls_first,
                                            NormalizedKeyIter normalized_key_iter) {
  // Normalized keys are ordered in ascendingly by default. The modifier is applied to invert this order.
  const auto modifier = (ascending) ? std::byte{0x00} : std::byte{0xFF};
  const auto full_modifier = (ascending) ? uint64_t{0x00} : std::numeric_limits<uint64_t>::max();
  const auto normalized_null_value = ((nulls_first) ? std::byte{0x00} : std::byte{0xFF});
  const auto normalized_non_null_value = ((nulls_first) ? std::byte{0xFF} : std::byte{0x00});
  segment_with_iterators<ColumnDataType>(segment, [&](auto it, const auto end) {
    while (it != end) {
      const auto segment_position = *it;
      DebugAssert(column_info.nullable || !segment_position.is_null(),
                  "Segment must be nullable or contains no null values");
      auto* normalized_key_start = normalized_key_iter->key_head + offset;

      // Encode the null byte for nullable segments. This byte is used to order null vs. non-null values (e.g.
      // put them to the front or end depending on the specified null order)
      if (column_info.nullable && segment_position.is_null()) {
        *normalized_key_start++ = normalized_null_value;
        // Initialize actual key by setting all bytes to 0.
        for (auto counter = size_t{0}; counter < column_info.encoding_width + column_info.extra_width; ++counter) {
          *normalized_key_start++ = std::byte{0};
        }
        DebugAssert(normalized_key_start == normalized_key_iter->key_head + offset + column_info.width(),
                    "Encoded unexpected number of bytes");
        ++it, ++normalized_key_iter;
        continue;
      }
      if (column_info.nullable) {
        *normalized_key_start++ = normalized_non_null_value;
      }

      const auto& value = segment_position.value();

      // Normalize value and encode to byte array.
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        for (const auto chr : value) {
          *normalized_key_start++ = static_cast<std::byte>(chr) ^ modifier;
        }
        // Add null bytes to pad values to correct size. (All keys must have the same size)
        for (auto counter = segment_position.value().size(); counter < column_info.encoding_width; ++counter) {
          *normalized_key_start++ = std::byte{0} ^ modifier;
        }
        copy_uint_to_byte_array(normalized_key_start, segment_position.value().size(), column_info.extra_width);
        normalized_key_start += column_info.extra_width;
      } else if constexpr (std::is_same_v<ColumnDataType, int32_t> || std::is_same_v<ColumnDataType, int64_t>) {
        using UnsignedColumnDataType = decltype(to_unsigned(ColumnDataType{}));
        static_assert(std::is_unsigned_v<UnsignedColumnDataType>, "Can not convert column data type to unsigned");

        // Encode numeric values to a byte array, because we want to compare these values we need to ensure that
        // the order is consistent after the conversion. Because of that, we will set the null value to half of the
        // numeric space. While this can be done by setting the highest bit to 1 for all positive values, negative
        // values are encoded as Two's complement and therefore need to be inverted. Because we use var int encoding,
        // the highest bit is determined by expected encoding length.
        //
        // Examples for int32_t:
        //
        // Number | Hex Representation | Unsinged (full) Representation | Unsigned (variable) Representation
        // -------|--------------------|--------------------------------|-----------------------------------
        //   -42  | 0xFFFFFFd6         | 0x7FFFFFd6                     | 0x56
        //    42  | 0x0000002a         | 0x8000002a                     | 0xaa

        auto unsigned_value = to_unsigned<ColumnDataType>(value);
        unsigned_value ^= UnsignedColumnDataType{1} << ((column_info.encoding_width * 8) - 1);
        unsigned_value ^= static_cast<UnsignedColumnDataType>(full_modifier);
        copy_uint_to_byte_array(normalized_key_start, unsigned_value, column_info.encoding_width);
        normalized_key_start += column_info.encoding_width;
      } else if constexpr (std::is_same_v<ColumnDataType, float> || std::is_same_v<ColumnDataType, double>) {
        using UnsignedColumnDataType = decltype(to_unsigned(ColumnDataType{value}));
        static_assert(std::is_unsigned_v<UnsignedColumnDataType>, "Can not convert column data type to unsigned");

        auto unsigned_value = to_unsigned<ColumnDataType>(ColumnDataType{value});
        if (value < 0) {
          unsigned_value ^= std::numeric_limits<UnsignedColumnDataType>::max();
        } else {
          unsigned_value |= UnsignedColumnDataType{1} << ((sizeof(UnsignedColumnDataType) * 8) - 1);
        }
        unsigned_value = byteswap(unsigned_value);
        memcpy(normalized_key_start, &unsigned_value, sizeof(UnsignedColumnDataType));
        normalized_key_start += sizeof(UnsignedColumnDataType);
      } else {
        Fail(std::format("Cannot encode values of type '{}'", typeid(ColumnDataType).name()));
      }
      if constexpr (HYRISE_DEBUG) {
        DebugAssert(normalized_key_start == normalized_key_iter->key_head + offset + column_info.width(),
                    "Encoded unexpected number of bytes");
      }
      ++it, ++normalized_key_iter;
    }
  });
}

template <typename T>
concept NormalizedKeyRange = std::ranges::range<T> && std::ranges::random_access_range<T> &&
                             std::same_as<std::ranges::range_value_t<T>, NormalizedKeyRow>;
template <typename Func>
concept NormalizedKeyComparator = requires(Func func, const NormalizedKeyRow& row) {
  { func(row, row) } -> std::same_as<bool>;
};

void insertion_sort(auto begin, auto end, const NormalizedKeyComparator auto& comp) {
  for (auto it = begin + 1; it < end; ++it) {
    auto current = *it;
    auto insert_pos = it;
    while (insert_pos != begin && comp(current, *(insert_pos - 1))) {
      *insert_pos = *(insert_pos - 1);
      --insert_pos;
    }
    *insert_pos = current;
  }
}

void radix_sort_lsd(auto begin, auto end, size_t normalized_key_size) {
  const auto total_size = static_cast<size_t>(std::distance(begin, end));
  if (total_size <= 1 || normalized_key_size == 0)
    return;

  auto counts = std::array<size_t, RADIX>{};
  auto offsets = std::array<size_t, RADIX>{};
  auto buffer = pmr_vector<NormalizedKeyRow>(total_size);

  for (ssize_t r = static_cast<ssize_t>(normalized_key_size) - 1; r >= 0; --r) {
    std::ranges::fill(counts, 0);

    for (auto it = begin; it != end; ++it)
      ++counts[static_cast<uint8_t>(it->key_head[r])];

    offsets[0] = 0;
    for (size_t i = 1; i < RADIX; ++i)
      offsets[i] = offsets[i - 1] + counts[i - 1];

    for (auto it = begin; it != end; ++it)
      buffer[offsets[static_cast<uint8_t>(it->key_head[r])]++] = *it;

    std::copy(buffer.begin(), buffer.end(), begin);
  }
}

void radix_sort_msd(auto begin, auto end, size_t byte_index, size_t max_key_size,
                    pmr_vector<NormalizedKeyRow>& buffer) {
  const auto total_size = static_cast<size_t>(std::distance(begin, end));
  if (total_size <= 1 || byte_index >= max_key_size)
    return;

  auto counts = std::array<size_t, RADIX>{};
  auto offsets = std::array<size_t, RADIX>{};

  for (auto it = begin; it != end; ++it)
    ++counts[static_cast<uint8_t>(it->key_head[byte_index])];

  offsets[0] = 0;
  for (auto i = size_t{1}; i < RADIX; ++i)
    offsets[i] = offsets[i - 1] + counts[i - 1];

  for (auto it = begin; it != end; ++it)
    buffer[offsets[static_cast<uint8_t>(it->key_head[byte_index])]++] = *it;

  std::copy(buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(total_size), begin);

  auto current_offset = size_t{0};
  for (auto i = size_t{0}; i < RADIX; ++i) {
    auto radix_count = counts[i];
    if (radix_count <= 1) {
      current_offset += radix_count;
      continue;
    }

    auto subrange_begin = begin + static_cast<std::ptrdiff_t>(current_offset);
    auto subrange_end = subrange_begin + static_cast<std::ptrdiff_t>(radix_count);

    if (radix_count < INSERTION_SORT_THRESHOLD) {
      insertion_sort(subrange_begin, subrange_end, [&](const NormalizedKeyRow& lhs, const NormalizedKeyRow& rhs) {
        return lhs.less_than(rhs, max_key_size);
      });
    } else {
      radix_sort_msd(subrange_begin, subrange_end, byte_index + 1, max_key_size, buffer);
    }

    current_offset += radix_count;
  }
}

void radix_sort(auto begin, auto end, size_t normalized_key_size, bool contains_string) {
  const auto total_size = static_cast<size_t>(std::distance(begin, end));
  if (total_size <= 1 || normalized_key_size == 0)
    return;

  const auto comparator = [&](const NormalizedKeyRow& lhs, const NormalizedKeyRow& rhs) {
    return lhs.less_than(rhs, normalized_key_size);
  };

  if (contains_string) {
    boost::sort::pdqsort(begin, end, comparator);
    return;
  }

  if (total_size < INSERTION_SORT_THRESHOLD) {
    insertion_sort(begin, end, comparator);
    return;
  }

  if (normalized_key_size <= MSD_RADIX_SORT_SIZE_THRESHOLD) {
    radix_sort_lsd(begin, end, normalized_key_size);
    return;
  }

  auto buffer = pmr_vector<NormalizedKeyRow>(total_size);
  radix_sort_msd(begin, end, 0, normalized_key_size, buffer);
}

void merge_partition(NormalizedKeyIter out_begin, NormalizedKeyIter a_begin, NormalizedKeyIter a_end,
                     NormalizedKeyIter b_begin, NormalizedKeyIter b_end, size_t normalized_key_size) {
  while (a_begin != a_end && b_begin != b_end) {
    if (b_begin->less_than(*a_begin, normalized_key_size)) {
      *out_begin++ = *b_begin++;
    } else {
      *out_begin++ = *a_begin++;
    }
  }
  out_begin = std::move(a_begin, a_end, out_begin);
  out_begin = std::move(b_begin, b_end, out_begin);
}

std::pair<NormalizedKeyIter, NormalizedKeyIter> merge_path_search(NormalizedKeyIter begin1, NormalizedKeyIter end1,
                                                                  NormalizedKeyIter begin2, NormalizedKeyIter end2,
                                                                  size_t diag, size_t normalized_key_size) {
  using Diff = std::ptrdiff_t;

  const Diff a_len = std::distance(begin1, end1);
  const Diff b_len = std::distance(begin2, end2);
  const Diff merge_index = static_cast<Diff>(diag);
  Diff low = std::max<Diff>(0, merge_index - b_len);
  Diff high = std::min<Diff>(merge_index, a_len);
  while (low < high) {
    Diff a_index = (low + high) / 2;
    Diff b_index = merge_index - a_index;

    if (a_index < a_len && b_index > 0 &&
        std::next(begin1, a_index)->less_than(*std::next(begin2, b_index - 1), normalized_key_size)) {
      low = a_index + 1;
    } else {
      high = a_index;
    }
  }
  return {begin1 + low, begin2 + (merge_index - low)};
}

void parallel_merge_sort(NormalizedKeyRange auto& sort_range, size_t normalized_key_size, bool contains_string) {
  TRACE_EVENT("sort", "parallel_merge_sort");
  const size_t total_size = sort_range.size();
  if (total_size <= 1 || normalized_key_size == 0)
    return;
  auto begin = sort_range.begin();
  auto end = sort_range.end();

  if (total_size < INSERTION_SORT_THRESHOLD) {
    const auto comparator = [&](const NormalizedKeyRow& lhs, const NormalizedKeyRow& rhs) {
      return lhs.less_than(rhs, normalized_key_size);
    };

    insertion_sort(begin, end, comparator);
    return;
  }

  const auto num_threads = std::thread::hardware_concurrency();
  const size_t min_block_size = (total_size + num_threads - 1) / num_threads;

  using Iter = typename std::remove_cvref_t<decltype(sort_range)>::iterator;
  using Value = typename Iter::value_type;

  const auto num_partitions = (total_size + min_block_size - 1) / min_block_size;
  std::vector<std::pair<Iter, Iter>> ranges(num_partitions);
  const auto sort_tasks = run_parallel_batched(num_partitions, 1, [&](auto index) {
    const auto offset = index * min_block_size;
    auto chunk_begin = begin + offset;
    auto chunk_end = (offset + min_block_size < total_size) ? (chunk_begin + min_block_size) : end;
    ranges[index] = {chunk_begin, chunk_end};

    radix_sort(chunk_begin, chunk_end, normalized_key_size, contains_string);
  });
  Hyrise::get().scheduler()->wait_for_tasks(sort_tasks);

  auto buffer = pmr_vector<Value>();
  buffer.reserve(std::ranges::size(sort_range));
  while (ranges.size() > 1) {
    const size_t num_pairs = ranges.size() / 2;
    const size_t new_size = num_pairs + (ranges.size() % 2);
    std::vector<std::pair<Iter, Iter>> new_ranges;
    new_ranges.reserve(new_size);

    for (size_t i = 0; i + 1 < ranges.size(); i += 2) {
      auto begin1 = ranges[i].first;
      auto end1 = ranges[i].second;
      auto end2 = ranges[i + 1].second;
      const size_t total_len = std::distance(begin1, end2);
      TRACE_EVENT_BEGIN("sort", "copy");
      if (total_len > 100000) {
        const auto block_size = total_len / num_threads;
        const auto copy_tasks = run_parallel_batched(num_threads, 1, [&](const auto index) {
          const auto total_offset = block_size * index;
          const auto block_begin = begin1 + (block_size * index);
          const auto block_end = (index + 1 == num_threads) ? end2 : block_begin + block_size;
          std::move(block_begin, block_end, buffer.begin() + total_offset);
        });
        Hyrise::get().scheduler()->wait_for_tasks(copy_tasks);
      } else {
        std::move(begin1, end2, buffer.begin());
      }
      TRACE_EVENT_END("sort");

      auto a_begin_buf = buffer.begin();
      auto a_end_buf = a_begin_buf + std::distance(begin1, end1);
      auto b_begin_buf = a_end_buf;
      auto b_end_buf = buffer.begin() + total_len;

      // Precompute partitions
      TRACE_EVENT_BEGIN("sort", "precompute partitions");
      std::vector<std::pair<decltype(a_begin_buf), decltype(b_begin_buf)>> partitions(num_threads + 1);
      for (size_t thread_idx = 0; thread_idx <= num_threads; ++thread_idx) {
        size_t diag = (thread_idx * total_len) / num_threads;
        partitions[thread_idx] =
            merge_path_search(a_begin_buf, a_end_buf, b_begin_buf, b_end_buf, diag, normalized_key_size);
      }
      TRACE_EVENT_END("sort");

      TRACE_EVENT_BEGIN("sort", "merge");
      const auto merge_tasks = run_parallel_batched(num_threads, 1, [&](auto thread_idx) {
        auto partition_start = partitions[thread_idx];
        auto a_start = partition_start.first;
        auto b_start = partition_start.second;

        auto partition_end = partitions[thread_idx + 1];
        auto a_end_part = partition_end.first;
        auto b_end_part = partition_end.second;
        auto out_begin = begin1 + ((thread_idx * total_len) / num_threads);

        merge_partition(out_begin, a_start, a_end_part, b_start, b_end_part, normalized_key_size);
      });
      Hyrise::get().scheduler()->wait_for_tasks(merge_tasks);
      TRACE_EVENT_END("sort");

      new_ranges.emplace_back(begin1, end2);
    }
    if (ranges.size() % 2 == 1) {
      new_ranges.emplace_back(ranges.back());
    }
    ranges = std::move(new_ranges);
  }
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
  TRACE_EVENT("sort", "execute");
  auto timer = Timer{};
  const auto& input_table = left_input_table();

  TRACE_EVENT_BEGIN("sort", "setup");

  for (const auto& column_sort_definition : _sort_definitions) {
    Assert(column_sort_definition.column != INVALID_COLUMN_ID, "Sort: Invalid column in sort definition");
    Assert(column_sort_definition.column < input_table->column_count(),
           "Sort: Column ID is greater than table's column count");
  }

  if (input_table->row_count() == 0) {
    if (_force_materialization == ForceMaterialization::Yes && input_table->type() == TableType::References) {
      return Table::create_dummy_table(input_table->column_definitions());
    }
    return input_table;
  }

  auto sorted_table = std::shared_ptr<Table>{};

  auto column_materializer =
      pmr_vector<std::function<void(const AbstractSegment&, size_t, bool, ScanResult, bool, NormalizedKeyIter)>>();
  column_materializer.reserve(_sort_definitions.size());
  for (const auto& column_sort_definition : _sort_definitions) {
    const auto column_data_type = input_table->column_data_type(column_sort_definition.column);
    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      column_materializer.emplace_back(materialize_segment_as_normalized_keys<ColumnDataType>);
    });
  }

  // const auto init_time = timer.lap();
  timer.lap();

  TRACE_EVENT_END("sort");

  // Scan all chunks for the maximum number of bytes necessary to represent all column values. The scanning is
  // done in parallel on multiple threads.

  const auto search_column_count = _sort_definitions.size();
  const auto chunk_count = input_table->chunk_count();

  auto chunk_stats = std::vector<std::vector<ScanResult>>(search_column_count, std::vector<ScanResult>(chunk_count));

  TRACE_EVENT_BEGIN("sort", "scan");

  auto contains_string_column = false;
  auto tasks_per_column = std::vector(search_column_count, std::vector<std::shared_ptr<AbstractTask>>());
  for (auto column_index = size_t{0}; column_index < search_column_count; ++column_index) {
    const auto column_id = _sort_definitions[column_index].column;
    const auto column_data_type = input_table->column_data_type(column_id);
    const auto batch_size = opt_batch_size(chunk_count, 8, (10 + search_column_count - 1) / search_column_count);
    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      contains_string_column = contains_string_column || std::is_same_v<ColumnDataType, pmr_string>;

      if constexpr (std::is_same_v<ColumnDataType, float> || std::is_same_v<ColumnDataType, double>) {
        // Always the same with.
        const auto column_is_nullable = input_table->column_is_nullable(column_id);
        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          chunk_stats[column_index][chunk_id] = {.encoding_width = sizeof(ColumnDataType),
                                                 .nullable = column_is_nullable};
        }
      } else {
        // Elements may have different size.
        tasks_per_column[column_index] = run_parallel_batched(
            chunk_count, batch_size, [column_id, column_index, &chunk_stats, &input_table](const auto chunk_index) {
              TRACE_EVENT_BEGIN("sort", "scan", "column_id", static_cast<uint16_t>(column_id));
              const auto chunk_id = static_cast<ChunkID>(chunk_index);
              const auto segment = input_table->get_chunk(chunk_id)->get_segment(column_id);
              chunk_stats[column_index][chunk_id] = scan_column<ColumnDataType>(*segment);
              TRACE_EVENT_END("sort");
            });
      }
    });
  }

  for (const auto& tasks : tasks_per_column) {
    if (!tasks.empty()) {
      Hyrise::get().scheduler()->wait_for_tasks(tasks);
    }
  }

  auto column_stats = std::vector<ScanResult>(search_column_count, ScanResult{});
  auto normalized_key_size = size_t{0};
  for (auto column_index = size_t{0}; column_index < search_column_count; ++column_index) {
    const auto column_id = _sort_definitions[column_index].column;
    TRACE_EVENT("sort", "collect_stats", "sort_column_index", column_index);
    const auto aggregated_stats = std::accumulate(chunk_stats[column_index].begin(), chunk_stats[column_index].end(),
                                                  ScanResult{}, [](ScanResult result, ScanResult chunk) {
                                                    return result.merge(chunk);
                                                  });
    Assert(aggregated_stats.width() > 0, std::format("Invalid width for column {}", static_cast<uint16_t>(column_id)));
    column_stats[column_index] = aggregated_stats;
    normalized_key_size += aggregated_stats.width();
  }

  TRACE_EVENT_END("sort");

  const auto padded_key_size = ((normalized_key_size + 3) / 4) * 4;
  std::cerr << "normalized_key_size " << normalized_key_size << "\n";
  std::cerr << "padded_key_size " << padded_key_size << "\n";

  // const auto scan_time = timer.lap();
  timer.lap();

  // Convert the columnar layout into a row layout for better sorting. This is done by encoding all sorted columns
  // into an array of bytes. These rows can be compared using memcmp.

  TRACE_EVENT_BEGIN("sort", "allocate_rows");

  auto materialized_rows = pmr_vector<NormalizedKeyRow>();
  materialized_rows.resize(input_table->row_count());

  TRACE_EVENT_END("sort");

  TRACE_EVENT_BEGIN("sort", "materialize");

  // Create list of chunks to materialize.
  auto total_offset = size_t{0};
  auto chunk_offsets = std::vector<size_t>(chunk_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk_size = input_table->get_chunk(chunk_id)->size();
    chunk_offsets[chunk_id] = total_offset;
    total_offset += chunk_size;
  }

  auto chunk_allocations = std::vector<pmr_vector<std::byte>>(chunk_count);
  const auto materialization_tasks = run_parallel_batched(chunk_count, 1, [&](const auto chunk_index) {
    const auto chunk_id = static_cast<ChunkID>(chunk_index);
    TRACE_EVENT("sort", "materialize_chunk", "chunk_id", static_cast<size_t>(chunk_id));
    const auto chunk = input_table->get_chunk(chunk_id);
    const auto chunk_size = chunk->size();
    chunk_allocations[chunk_id] = pmr_vector<std::byte>(chunk_size * padded_key_size);

    auto normalized_key_iter = materialized_rows.begin() + chunk_offsets[chunk_index];
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      const auto row_id = RowID{chunk_id, chunk_offset};
      *normalized_key_iter++ = NormalizedKeyRow{
          .key_head = chunk_allocations[chunk_id].data() + (chunk_offset * padded_key_size),
          .row_id = row_id,
      };
    }

    normalized_key_iter = materialized_rows.begin() + chunk_offsets[chunk_index];
    auto key_offset = size_t{0};
    for (auto index = size_t{0}; index < search_column_count; ++index) {
      TRACE_EVENT("sort", "materialize_column", "sort_column_index", index);
      const auto sort_mode = _sort_definitions[index].sort_mode;
      const auto column_id = _sort_definitions[index].column;
      const auto segment = chunk->get_segment(column_id);
      const auto ascending = sort_mode == SortMode::AscendingNullsFirst || sort_mode == SortMode::AscendingNullsLast;
      const auto nulls_first =
          sort_mode == SortMode::AscendingNullsFirst || sort_mode == SortMode::DescendingNullsFirst;
      DebugAssert(index < column_materializer.size(), "Search column out of bounds");
      auto materializer = column_materializer[index];
      materializer(*segment, key_offset, ascending, column_stats[index], nulls_first, normalized_key_iter);
      key_offset += column_stats[index].width();
    }
  });
  Hyrise::get().scheduler()->wait_for_tasks(materialization_tasks);

  TRACE_EVENT_END("sort");

  const auto materialization_time = timer.lap();

  TRACE_EVENT_BEGIN("sort", "sort");

  // TODO(student): Use pdqsort

  parallel_merge_sort(materialized_rows, normalized_key_size, contains_string_column);

  TRACE_EVENT_END("sort");

  const auto sort_time = timer.lap();

  TRACE_EVENT_BEGIN("sort", "write_back");

  // Extract the positions from the sorted rows.
  auto position_list = RowIDPosList();
  position_list.reserve(materialized_rows.size());
  for (const auto& row : materialized_rows) {
    position_list.push_back(row.row_id);
  }

  TRACE_EVENT_END("sort");

  const auto write_back_time = timer.lap();

  // TODO(student): Update performance metrics.
  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  step_performance_data.set_step_runtime(OperatorSteps::MaterializeSortColumns, materialization_time);
  step_performance_data.set_step_runtime(OperatorSteps::TemporaryResultWriting, write_back_time);
  step_performance_data.set_step_runtime(OperatorSteps::Sort, sort_time);

  TRACE_EVENT("sort", "write_output");
  // We have to materialize the output (i.e., write ValueSegments) if
  //  (a) it is requested by the user,
  //  (b) a column in the table references multiple tables (see write_reference_output_table for details), or
  //  (c) a column in the table references multiple columns in the same table (which is an unlikely edge case).
  // Cases (b) and (c) can only occur if there is more than one ReferenceSegment in an input chunk.
  auto must_materialize = _force_materialization == ForceMaterialization::Yes;
  const auto input_chunk_count = input_table->chunk_count();
  if (!must_materialize && input_table->type() == TableType::References && input_chunk_count > 1) {
    TRACE_EVENT("sort", "write_prepare");
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
    sorted_table = write_materialized_output_table(input_table, std::move(position_list), _output_chunk_size);
  } else {
    sorted_table = write_reference_output_table(input_table, std::move(position_list), _output_chunk_size);
  }

  const auto& final_sort_definition = _sort_definitions[0];
  // Set the sorted_by attribute of the output's chunks according to the most significant sort operation, which is the
  // column the table was sorted by last.
  const auto output_chunk_count = sorted_table->chunk_count();
  for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
    const auto& output_chunk = sorted_table->get_chunk(output_chunk_id);
    output_chunk->set_immutable();
    output_chunk->set_individually_sorted_by(final_sort_definition);
  }

  step_performance_data.set_step_runtime(OperatorSteps::WriteOutput, timer.lap());
  return sorted_table;
}

void perfetto_run(const std::shared_ptr<const Table>& input_table,
                  const std::vector<SortColumnDefinition>& sort_definitions) {
  for (auto index = size_t{0}; index < 5; ++index) {
    const auto table_wrapper = std::make_shared<TableWrapper>(input_table);
    table_wrapper->execute();
    const auto sort_operator =
        std::make_shared<Sort>(table_wrapper, sort_definitions, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No);

#ifdef ENABLE_PERFETTO
    auto track_event_cfg = perfetto::protos::gen::TrackEventConfig{};
    track_event_cfg.add_enabled_categories("sort");

    auto args = perfetto::TracingInitArgs{};
    args.backends = perfetto::kInProcessBackend;
    perfetto::Tracing::Initialize(args);
    perfetto::TrackEvent::Register();

    auto cfg = perfetto::TraceConfig{};
    cfg.add_buffers()->set_size_kb(4096);
    auto* ds_cfg = cfg.add_data_sources()->mutable_config();
    ds_cfg->set_name("track_event");
    ds_cfg->set_track_event_config_raw(track_event_cfg.SerializeAsString());

    auto tracing_session = std::unique_ptr<perfetto::TracingSession>(perfetto::Tracing::NewTrace());
    tracing_session->Setup(cfg);
    tracing_session->StartBlocking();
#endif

    auto timer = Timer{};
    sort_operator->execute();
    std::cout << timer.lap() << "\n";

#ifdef ENABLE_PERFETTO
    tracing_session->StopBlocking();

    auto trace_data = std::vector<char>(tracing_session->ReadTraceBlocking());
    auto output = std::ofstream{};
    output.open(std::format("traces/dyod2025_sort_operator_{}.perfetto-trace", index),
                std::ios::out | std::ios::binary);
    output.write(trace_data.data(), static_cast<std::streamsize>(trace_data.size()));
    output.close();
#endif
  }
}

}  // namespace hyrise
