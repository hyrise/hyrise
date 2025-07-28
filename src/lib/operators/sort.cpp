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
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <fstream>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
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

#define DEBUG(v) #v "=" << v

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
  std::byte* key_head = nullptr;
  RowID row_id;

  bool less_than(const NormalizedKeyRow& other, size_t expected_size) const {
    if (expected_size == 0) {
      return false;
    }
    DebugAssert(key_head, "NormalizedKey is not valid");
    DebugAssert(other.key_head, "NormalizedKey is not valid");
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

namespace ips4o {

template <typename T>
concept NormalizedKeyRange = std::ranges::range<T> && std::ranges::random_access_range<T> &&
                             std::same_as<std::ranges::range_value_t<T>, NormalizedKeyRow>;
template <typename Func>
concept NormalizedKeyComparator = requires(Func func, const NormalizedKeyRow& row) {
  { func(row, row) } -> std::same_as<bool>;
};

// Result of the classification of a stripe.
struct StripeClassificationResult {
  StripeClassificationResult() = default;

  StripeClassificationResult(size_t num_buckets, size_t block_size)
      : buckets(num_buckets, std::vector<NormalizedKeyRow>()), bucket_sizes(num_buckets, 0) {
    for (auto bucket_index = size_t{0}; bucket_index < num_buckets; ++bucket_index) {
      buckets[bucket_index].reserve(block_size);
    }
  }

  std::vector<std::vector<NormalizedKeyRow>> buckets;
  std::vector<std::size_t> bucket_sizes;
  // Number of blocks written back to the stripe.
  size_t num_blocks_written = 0;
};

template <typename It>
class AtomicIteratorHelper {
 public:
  AtomicIteratorHelper() = default;

  void init(It base, int64_t offset, size_t block_size) {
    this->base = base;
    std::atomic_init(&this->offset, offset);
    this->block_size = block_size;
  }

  // Atomic increment to the next block.
  std::ranges::subrange<It> operator++() {
    const auto begin_offset = offset.fetch_add(block_size, std::memory_order_relaxed);
    const auto block_begin = std::next(base, begin_offset);
    const auto block_end = std::next(block_begin, block_size);
    return std::ranges::subrange(block_begin, block_end);
  }

  // Atomic decrement to the previous block.
  std::ranges::subrange<It> operator--() {
    const auto begin_offset = offset.fetch_sub(block_size, std::memory_order_relaxed);
    const auto block_begin = std::next(base, begin_offset);
    const auto block_end = std::next(block_begin, block_size);
    return std::ranges::subrange(block_begin, block_end);
  }

  ssize_t distance_to_start(NormalizedKeyRange auto& range) const {
    return std::distance(range.begin(), base) + offset.load(std::memory_order_relaxed);
  }

  It base;
  std::atomic_int64_t offset;
  size_t block_size = 0;
};

template <typename It>
std::strong_ordering operator<=>(const AtomicIteratorHelper<It>& left, const AtomicIteratorHelper<It>& right) {
  DebugAssert(left.base == right.base, "Both helper must use the same base");
  return left.offset.load(std::memory_order_relaxed) <=> right.offset.load(std::memory_order_relaxed);
}

template <typename It>
std::strong_ordering operator<=>(const std::ranges::subrange<It>& block, const AtomicIteratorHelper<It>& right) {
  DebugAssert(std::ranges::size(block) == right.block_size, "Expected block as left comparator");
  const auto offset = std::distance(right.base, block.begin());
  return offset <=> right.offset.load(std::memory_order_relaxed);
}

/*

void debug_print_keys(const auto& name, const NormalizedKeyRange auto& range) {
  if constexpr (HYRISE_DEBUG) {
    std::cout << name << " " << std::hex;
    for (const auto& row : range) {
      std::cout << "0x";
      for (auto counter = size_t{0}; counter < 2; ++counter) {
        std::cout << std::setfill('0') << std::setw(2) << static_cast<int32_t>(row.key_head[counter]);
      }
      std::cout << " ";
    }
    std::cout << "\n" << std::dec;
  }
}

void debug_print_values(const auto& name, const std::ranges::range auto& range) {
  if constexpr (HYRISE_DEBUG) {
    std::cout << name << " ";
    for (const auto& value : range) {
      std::cout << value << " ";
    }
    std::cout << "\n";
  }
}

*/

// Divide left by right and round up to the next larger integer.
auto div_ceil(const auto left, const auto right) {
  return (left + right - 1) / right;
}

// Returns the next multiple. If value is already a multiple, then return value.
auto next_multiple(const auto value, const auto multiple) {
  return div_ceil(value, multiple) * multiple;
}

// Select the classifiers for the sample sort. At the moment this selects first num classifiers many keys.
std::vector<NormalizedKeyRow> select_classifiers(const NormalizedKeyRange auto& sort_range, size_t num_classifiers,
                                                 size_t samples_per_classifier,
                                                 const NormalizedKeyComparator auto& comp) {
  TRACE_EVENT("sort", "ips4o::select_classifiers");

  const auto size = std::ranges::size(sort_range);
  const auto num_samples = num_classifiers * samples_per_classifier;
  const auto elements_per_sample = size / num_samples;
  const auto offset = elements_per_sample / 2;

  auto samples = std::vector<NormalizedKeyRow>(num_samples);
  for (auto sample = size_t{0}; sample < num_samples; ++sample) {
    const auto index = (sample * elements_per_sample) + offset;
    DebugAssert(index < size, "Index out of range");
    samples[sample] = *std::next(sort_range.begin(), index);
  }
  boost::sort::pdqsort(samples.begin(), samples.end(), comp);

  const auto samples_offset = samples_per_classifier / 2;
  auto classifiers = std::vector<NormalizedKeyRow>(num_classifiers);
  for (auto classifier = size_t{0}; classifier < num_classifiers; ++classifier) {
    const auto sample = (classifier * samples_per_classifier) + samples_offset;
    DebugAssert(sample < num_samples, "Index out of range");
    classifiers[classifier] = samples[sample];
  }

  return classifiers;
}

// Return the index of the bucket an element belongs to.
size_t classify_value(const NormalizedKeyRow& value, const NormalizedKeyRange auto& classifiers,
                      const NormalizedKeyComparator auto& comp) {
  const auto it = std::ranges::lower_bound(classifiers, value, comp);
  const auto result = std::distance(classifiers.begin(), it);
  return result;
}

/*
 * Classify all elements of stripe_range into buckets. Each bucket holds at most block size many elements. A full
 * bucket is written back to the start of the stripe range.
 */
StripeClassificationResult classify_stripe(NormalizedKeyRange auto& stripe_range,
                                           const NormalizedKeyRange auto& classifiers, const size_t block_size,
                                           const NormalizedKeyComparator auto& comp) {
  TRACE_EVENT("sort", "ips4o::classify_stripe");
  const auto num_buckets = std::ranges::size(classifiers) + 1;
  DebugAssert(num_buckets > 0, "At least one bucket is required");
  DebugAssert(block_size > 0, "Invalid bock size");
  auto result = StripeClassificationResult(num_buckets, block_size);
  auto write_back_begin = std::ranges::begin(stripe_range);
  for (const auto& key : stripe_range) {
    const auto bucket_index = classify_value(key, classifiers, comp);
    DebugAssert(bucket_index < num_buckets, "Bucket index out of range");
    result.buckets[bucket_index].push_back(key);
    ++result.bucket_sizes[bucket_index];
    if (result.buckets[bucket_index].size() == block_size) {
      std::ranges::copy(result.buckets[bucket_index], write_back_begin);
      write_back_begin = std::next(write_back_begin, block_size);
      result.buckets[bucket_index].clear();
      ++result.num_blocks_written;
    }
  }
  return result;
}

// Count the number of empty blocks the specified bucket overlaps with in the provided stripe.
size_t count_empty_blocks(const size_t stripe_size, const ssize_t stripe_end, const ssize_t last_bucket_begin,
                          const StripeClassificationResult& classification_result, size_t block_size) {
  const auto stripe_num_blocks = stripe_size / block_size;
  // DebugAssert(stripe_num_blocks * block_size == stripe_size, "Stripes should be block size");
  const auto stripe_empty_blocks = stripe_num_blocks - classification_result.num_blocks_written;
  const auto count_to_stripe_end = last_bucket_begin - stripe_end;
  const auto blocks_to_stripe_end = static_cast<ssize_t>(count_to_stripe_end / block_size);
  // DebugAssert(static_cast<ssize_t>(blocks_to_stripe_end * block_size) == count_to_stripe_end,
  //             "Everything should be aligned to blocks");
  return std::min(blocks_to_stripe_end, static_cast<ssize_t>(stripe_empty_blocks));
}

/**
 * Fill the gaps inside a bucket so that the first blocks are classified and the last blocks are empty. Only buckets crossing chunks boundaries need to be fixed.
 */
void prepare_block_permutations(NormalizedKeyRange auto& sort_range, size_t stripe_index,
                                const std::ranges::range auto& stripe_ranges,
                                const std::ranges::range auto& stripe_results,
                                const std::ranges::range auto& bucket_delimiter, const size_t block_size) {
  const auto stripe_result = std::next(stripe_results.begin(), stripe_index);
  const auto stripe_range_iter = std::next(stripe_ranges.begin(), stripe_index);
  const auto stripe_end = stripe_range_iter->end();
  const auto stripe_end_index = std::distance(sort_range.begin(), stripe_end);

  // Find the first bucket ending after the end of this stripe.
  const auto last_bucket_iter =
      std::ranges::lower_bound(bucket_delimiter, stripe_end_index, [](const ssize_t stripe_end, const auto bucket_end) {
        return stripe_end < static_cast<ssize_t>(bucket_end);
      });
  const auto last_bucket_index = std::distance(bucket_delimiter.begin(), last_bucket_iter);
  const auto last_bucket_begin =
      (last_bucket_index != 0) ? static_cast<ssize_t>(bucket_delimiter[last_bucket_index - 1]) : ssize_t{0};
  const auto last_bucket_end = static_cast<ssize_t>(bucket_delimiter[last_bucket_index]);
  // Find the stripe the last bucket starts in.
  const auto last_bucket_first_stripe =
      std::ranges::lower_bound(stripe_ranges, last_bucket_begin, std::less<ssize_t>(), [&](const auto& stripe_range) {
        return std::distance(sort_range.begin(), stripe_range.end());
      });
  const auto last_bucket_first_stripe_index = std::distance(stripe_ranges.begin(), last_bucket_first_stripe);

  // Check if the bucket starts in the next stripe. This can happen if the bucket delimiter aligns with the stripe end.
  if (static_cast<ssize_t>(stripe_index) < last_bucket_first_stripe_index) {
    return;
  }

  auto num_already_moved_blocks = ssize_t{0};
  auto stripe_result_iter = std::next(stripe_results.begin(), last_bucket_first_stripe_index);
  for (auto stripe_iter = last_bucket_first_stripe; stripe_iter != stripe_range_iter;
       ++stripe_iter, ++stripe_result_iter) {
    const auto stripe_size = std::ranges::size(*stripe_iter);
    const auto stripe_end = std::distance(std::ranges::begin(sort_range), stripe_iter->end());
    // At these empty to blocks to the number of already processed blocks, because they are processed by different
    // threads.
    num_already_moved_blocks +=
        count_empty_blocks(stripe_size, stripe_end, last_bucket_begin, *stripe_result_iter, block_size);
  }

  // Calculate the number of blocks which must be filled by this stripe.
  const auto stripe_size = std::ranges::size(*stripe_range_iter);
  auto move_num_blocks = static_cast<ssize_t>(
      count_empty_blocks(stripe_size, stripe_end_index, last_bucket_begin, *stripe_result, block_size));
  auto stripe_empty_begin = std::next(stripe_range_iter->begin(), stripe_result->num_blocks_written * block_size);

  // Find last stripe a bucket is in.
  auto last_stripe =
      std::ranges::lower_bound(stripe_ranges, last_bucket_end, std::less<ssize_t>(), [&](const auto& stripe_range) {
        return std::distance(sort_range.begin(), stripe_range.end());
      });
  if (last_stripe == stripe_ranges.end()) {
    last_stripe = --stripe_ranges.end();
  }
  DebugAssert(last_stripe != stripe_ranges.end(), "Bucket should end in last stripe");
  const auto last_stripe_index = std::distance(stripe_ranges.begin(), last_stripe);
  auto last_stripe_result = std::next(stripe_results.begin(), last_stripe_index);

  // Copy the last blocks in the bucket to the empty blocks of this stripe. Skip num_already_moved_blocks before moving
  // blocks from the end into the empty blocks, because they are already processed by previous stripes.
  for (; last_stripe != stripe_range_iter && move_num_blocks > 0; --last_stripe, --last_stripe_result) {
    const auto begin_index = std::distance(sort_range.begin(), last_stripe->begin());
    DebugAssert(begin_index % block_size == 0, "Wrong alignment");
    const auto stripe_end_index = std::distance(sort_range.begin(), last_stripe->end());
    const auto end_index = std::min(last_bucket_end, stripe_end_index);
    const auto bucket_num_blocks = (end_index - begin_index) / block_size;
    const auto num_blocks_written = last_stripe_result->num_blocks_written;
    auto num_moveable_blocks = static_cast<ssize_t>(std::min(bucket_num_blocks, num_blocks_written));

    if (num_already_moved_blocks >= num_moveable_blocks) {
      num_already_moved_blocks -= num_moveable_blocks;
      continue;
    }
    num_moveable_blocks -= num_already_moved_blocks;
    const auto blocks_to_move = std::min(num_moveable_blocks, move_num_blocks);

    auto stripe_full_begin = std::next(last_stripe->begin(), (num_moveable_blocks - 1) * block_size);
    for (auto counter = ssize_t{0}; counter < blocks_to_move; ++counter) {
      const auto block_end = std::next(stripe_full_begin, block_size);
      std::copy(stripe_full_begin, block_end, stripe_empty_begin);
      stripe_empty_begin = std::next(stripe_empty_begin, block_size);
      stripe_full_begin = std::next(stripe_full_begin, -block_size);
    }
    move_num_blocks -= blocks_to_move;
    num_already_moved_blocks = 0;
  }
}

/**
 * Move classified blocks into the correct bucket.
 */
void permute_blocks(const NormalizedKeyRange auto& classifiers, const size_t initial_bucket, auto& write_pointers,
                    auto& read_pointers, std::vector<std::atomic_size_t>& pending_reads,
                    const NormalizedKeyComparator auto& comp, size_t block_size,
                    std::vector<NormalizedKeyRow>& overflow_bucket, const auto overflow_bucket_begin) {
  auto target_buffer = std::vector<NormalizedKeyRow>(block_size);
  auto swap_buffer = std::vector<NormalizedKeyRow>(block_size);

  const auto num_buckets = write_pointers.size();
  // Cycle through each bucket.
  auto first = true;
  for (auto bucket = initial_bucket; bucket != initial_bucket || first;
       bucket = (bucket + 1) % num_buckets, first = false) {
    while (write_pointers[bucket] <= read_pointers[bucket]) {
      // Try to acquire a block to read and read this buffer into the target buffer.
      pending_reads[bucket].fetch_add(1, std::memory_order_relaxed);
      const auto block = --read_pointers[bucket];
      if (write_pointers[bucket] > block) {
        // This block is already swapped out by another process.
        pending_reads[bucket].fetch_sub(1, std::memory_order_relaxed);
        break;
      }
      std::ranges::copy(block, target_buffer.begin());
      pending_reads[bucket].fetch_sub(1, std::memory_order_relaxed);

      // Classify the block.
      auto target_bucket = classify_value(target_buffer[0], classifiers, comp);
      // Acquire a block to write the buffer to. If the target block is not in the correct bucket both blocks are
      // swapped. Repeat until the target buffer is empty.
      while (true) {
        const auto target_block = ++write_pointers[target_bucket];
        DebugAssert(std::ranges::size(target_block) == block_size, "Block expected to be block size");
        if (target_block > read_pointers[target_bucket]) {
          // Read and write pointers has crossed each other. To avoid any race conditions, we wait until all reads are
          // complete.
          while (pending_reads[target_bucket].load(std::memory_order_relaxed) > 0) {}

          if (target_block.begin() == overflow_bucket_begin) {
            // Special Case: Let assume we have the following array to sort: [a a b c c]. In addition, we assume that
            // the bucket size is 2. A array with delimiter may look like [a a|b _|c c], but this is array is one
            // element longer than the original array. Because of that we provide an overflow bucket, which can be
            // written to in this case.
            overflow_bucket.resize(block_size);
            std::ranges::copy(target_buffer, overflow_bucket.begin());
          } else {
            std::ranges::copy(target_buffer, target_block.begin());
          }
          // The target buffer is now empty.
          break;
        }

        auto swap_block_bucket = classify_value(*target_block.begin(), classifiers, comp);
        if (swap_block_bucket == target_bucket) {
          // This block is already in the correct bucket. Try the next block.
          continue;
        }
        std::ranges::copy(target_block, swap_buffer.begin());
        std::ranges::copy(target_buffer, target_block.begin());
        std::swap(target_buffer, swap_buffer);
        target_bucket = swap_block_bucket;
      }
    }
  }
}

// Split the range at the specified index.
auto split_range_n(std::ranges::range auto& range, size_t index) {
  DebugAssert(index <= std::ranges::size(range), "Split index out of range");
  auto mid = std::next(range.begin(), index);
  return std::pair(std::ranges::subrange(range.begin(), mid), std::ranges::subrange(mid, range.end()));
}

// Removes the first index many elements.
auto cut_range_n(std::ranges::range auto& range, size_t index) {
  DebugAssert(index <= std::ranges::size(range), "Split index out of range");
  auto mid = std::next(range.begin(), index);
  return std::ranges::subrange(mid, range.end());
}

/**
 * Copy the values of the provide input range to the first and second output range. The first range is filled before
 * the second range and both are updated to only range above non-written values.
 */
void write_to_ranges(std::ranges::range auto& input, std::ranges::range auto& first, std::ranges::range auto& second) {
  const auto input_size = std::ranges::size(input);
  const auto first_size = std::ranges::size(first);
  DebugAssert(input_size <= first_size + std::ranges::size(second),
              "Cannot copy more values than first and second can hold");

  const auto copy_to_first = std::min(input_size, first_size);
  const auto [input_first, input_second] = split_range_n(input, copy_to_first);
  std::ranges::copy(input_first, first.begin());
  first = cut_range_n(first, copy_to_first);

  const auto copy_to_second = input_size - copy_to_first;
  DebugAssert(std::ranges::size(input_second) == copy_to_second, "Unexpected second input size");
  if (copy_to_second > 0) {
    std::ranges::copy(input_second, second.begin());
    second = cut_range_n(second, copy_to_second);
  }
}

/**
 * Implementation of IPS4o (https://arxiv.org/pdf/2009.13569) with a few modifications:
 * 1. Instead of using single threaded IPS4o this algorithm falls back to pdqsort.
 * 2. Instead of allocating an overflow bucket the overflow elements are handled extra.
 *
 * @param sort_range             Range of values to sort.
 * @param num_samples            Number of classifiers to be used for the sample sort.
 * @param block_size             Number of sequential elements to group into a block.
 * @param min_blocks_per_stripe  Minimum number of extra blocks before creating a new stripe.
 * @param max_parallelism        Maximum number of parallel tasks to spawn.
 * @param comp                   Comparator
 */
void ips4o_sort(NormalizedKeyRange auto& sort_range, const size_t num_buckets, const size_t samples_per_classifiers,
                const size_t block_size, const size_t min_blocks_per_stripe, size_t max_parallelism,
                const NormalizedKeyComparator auto& comp) {
  Assert(num_buckets > 1, "At least two buckets are required");
  TRACE_EVENT("sort", "ips4o");
  using RangeIterator = decltype(std::ranges::begin(sort_range));
  // The following terms are used in this algorithm:
  // - *block* A number of sequential elements. The sort_range is split up into these blocks.
  // - *stripe* A stripe is a set of sequential blocks. Each stripe is executed in its own task.

  DebugAssert(block_size > 0, "A block must contains at least one element.");
  const auto total_size = std::ranges::size(sort_range);
  const auto num_blocks = div_ceil(total_size, block_size);
  const auto num_classifiers = num_buckets - 1;
  // Calculate the number of blocks assigned to each stripe.
  const auto max_num_stripes = div_ceil(num_blocks, min_blocks_per_stripe);
  const auto num_stripes = std::min(max_num_stripes, max_parallelism);
  if (total_size <= block_size || num_stripes <= 1) {
    // Fallback to pdqsort.
    boost::sort::pdqsort(sort_range.begin(), sort_range.end(), comp);
    return;
  }
  const auto max_blocks_per_stripe = div_ceil(num_blocks, num_stripes);

  DebugAssert(num_blocks > 0, "At least one block is required.");
  DebugAssert(num_buckets > 0, "At least one bucket is required.");

  // Select the elements for the sample sort.
  const auto classifiers = select_classifiers(sort_range, num_classifiers, samples_per_classifiers, comp);

  // Create an array of elements assigned to each stripe. Equally distribute the blocks to each stripe.
  auto num_max_sized_stripes = num_blocks % num_stripes;
  if (num_max_sized_stripes == 0) {
    num_max_sized_stripes = num_stripes;
  }
  auto stripes = std::vector<std::ranges::subrange<RangeIterator>>(num_stripes);
  auto stripe_begin = std::ranges::begin(sort_range);
  for (auto stripe = size_t{0}; stripe < num_stripes - 1; ++stripe) {
    auto stripe_size = (max_blocks_per_stripe - 1) * block_size;
    if (num_max_sized_stripes > 0) {
      stripe_size += block_size;
      --num_max_sized_stripes;
    }
    auto stripe_end = std::next(stripe_begin, stripe_size);
    stripes[stripe] = std::ranges::subrange(stripe_begin, stripe_end);
    stripe_begin = stripe_end;
  }
  stripes.back() = std::ranges::subrange(stripe_begin, std::ranges::end(sort_range));

  // Classify all stripes. After this step each stripe consists of blocks where all elements are classified into the
  // same bucket and followed by empty blocks.
  TRACE_EVENT_BEGIN("sort", "ips4o::classify");
  auto classification_results = std::vector<StripeClassificationResult>(num_stripes);
  const auto classification_tasks = run_parallel_batched(num_stripes, 1, [&](auto stripe) {
    classification_results[stripe] = classify_stripe(stripes[stripe], classifiers, block_size, comp);
  });
  Hyrise::get().scheduler()->wait_for_tasks(classification_tasks);
  TRACE_EVENT_END("sort");

  // Prepare permuting classified blocks into the correct position. This preparation is done in the following:
  // 1. We create the prefix sum of all buckets. As a result, we receive a list of bucket end indices.
  // 2. We align these indices to the next block size.
  // 3. We move all blocks so that the following conditions is met for all buckets: Full blocks are followed by empty
  //    blocks (i.e., first we have the classified blocks followed by non written blocks).

  // Calculate prefix sum of all buckets and align the to the next block.
  auto aggregated_bucket_sizes = std::vector(num_buckets, size_t{0});
  auto aligned_bucket_sizes = std::vector(num_buckets, size_t{0});
  auto buckets_total_size = size_t{0};
  for (auto bucket_index = size_t{0}; bucket_index < num_buckets; ++bucket_index) {
    for (const auto& classification : classification_results) {
      buckets_total_size += classification.bucket_sizes[bucket_index];
    }
    aggregated_bucket_sizes[bucket_index] = buckets_total_size;
    aligned_bucket_sizes[bucket_index] = next_multiple(buckets_total_size, block_size);
  }

  TRACE_EVENT_BEGIN("sort", "ips4o::prepare_permutations");
  const auto prepare_tasks = run_parallel_batched(num_stripes, 1, [&](auto stripe) {
    prepare_block_permutations(sort_range, stripe, stripes, classification_results, aligned_bucket_sizes, block_size);
  });
  Hyrise::get().scheduler()->wait_for_tasks(prepare_tasks);
  TRACE_EVENT_END("sort");

  auto written_blocks_per_stripe = std::vector<size_t>(num_stripes);
  auto empty_blocks_per_stripe = std::vector<size_t>(num_stripes);
  for (auto stripe = size_t{0}; stripe < num_stripes; ++stripe) {
    const auto begin_index = std::distance(sort_range.begin(), stripes[stripe].begin());
    const auto end_index = std::distance(sort_range.begin(), stripes[stripe].end());
    const auto num_blocks = (end_index - begin_index) / block_size;
    const auto written_blocks = classification_results[stripe].num_blocks_written;
    empty_blocks_per_stripe[stripe] = num_blocks - written_blocks;
    written_blocks_per_stripe[stripe] = written_blocks;
  }

  // The write pointer is initialized to the first block of each bucket and the read pointer is initialized to the last
  // non-empty block of each bucket. Because each bucket, contains the classified elements first, all elements between
  // the write and read pointer (inclusive) are classified.
  TRACE_EVENT_BEGIN("sort", "ips4o::setup_pointers");
  auto write_pointers = std::vector<AtomicIteratorHelper<RangeIterator>>(num_buckets);
  auto read_pointers = std::vector<AtomicIteratorHelper<RangeIterator>>(num_buckets);
  auto pending_reads = std::vector<std::atomic_size_t>(num_buckets);
  auto bucket_begin = sort_range.begin();
  for (auto bucket = size_t{0}, stripe = size_t{0}; bucket < num_buckets; ++bucket) {
    write_pointers[bucket].init(bucket_begin, int64_t{0}, block_size);

    const auto delimiter_begin = std::distance(sort_range.begin(), bucket_begin);
    DebugAssert(delimiter_begin % block_size == 0, "Delimiter is misaligned");
    const auto delimiter_end = static_cast<ssize_t>(aligned_bucket_sizes[bucket]);
    DebugAssert(delimiter_end % block_size == 0, "Delimiter is misaligned");

    // Sum the number of blocks written to this bucket.
    auto num_blocks = ssize_t{0};
    while (stripe < num_stripes) {
      const auto stripe_begin = std::distance(sort_range.begin(), stripes[stripe].begin());
      DebugAssert(stripe_begin % block_size == 0, "Stripe begin is misaligned");
      const auto stripe_end = std::distance(sort_range.begin(), stripes[stripe].end());
      const auto stripe_written_end =
          static_cast<ssize_t>(stripe_begin + (block_size * classification_results[stripe].num_blocks_written));
      DebugAssert(stripe_written_end % block_size == 0, "Stripe end is misaligned");

      const auto written_begin = std::max(stripe_begin, delimiter_begin);
      // The delimiter may start after the last block is written. To avoid negative values we set the minimal value to
      // the delimiter_begin.
      const auto written_end = std::max(std::min(stripe_written_end, delimiter_end), delimiter_begin);

      const auto num_written = (written_end - written_begin);
      DebugAssert(num_written % block_size == 0, "Fence");
      num_blocks += num_written / block_size;

      if (delimiter_end <= stripe_end) {
        break;  // Last stripe of this bucket.
      }
      ++stripe;
    }

    // Set the offset to the first value of the last block.
    const auto read_pointer_offset = (num_blocks - 1) * static_cast<ssize_t>(block_size);

    read_pointers[bucket].init(bucket_begin, read_pointer_offset, block_size);
    bucket_begin = std::next(sort_range.begin(), delimiter_end);
  }
  TRACE_EVENT_END("sort");

  TRACE_EVENT_BEGIN("sort", "ips4o::permute_blocks");
  const auto overflow_bucket_offset = (std::ranges::size(sort_range) / block_size) * block_size;
  const auto overflow_bucket_begin = std::next(sort_range.begin(), overflow_bucket_offset);
  auto overflow_bucket = std::vector<NormalizedKeyRow>();

  // Move blocks into the correct bucket (based on the aligned bucket delimiter).
  const auto permute_blocks_tasks = run_parallel_batched(num_stripes, num_stripes, [&](auto stripe) {
    const auto initial_bucket = stripe % num_buckets;
    permute_blocks(classifiers, initial_bucket, write_pointers, read_pointers, pending_reads, comp, block_size,
                   overflow_bucket, overflow_bucket_begin);
  });
  Hyrise::get().scheduler()->wait_for_tasks(permute_blocks_tasks);
  TRACE_EVENT_END("sort");

  // Until this point all buckets are aligned to the block_size and there still some classified elements in the stripe
  // local buffer left. Because of that, we know move elements overflowing the bucket to the start of that bucket and
  // copy the values from the stripe classification result to empty elements at the buffer begin/end.

  TRACE_EVENT_BEGIN("sort", "ips4o::prepare_cleanup");
  // Create a range of empty elements at the start of each bucket. This cannot be done in parallel.
  auto bucket_empty_start = std::vector<std::ranges::subrange<RangeIterator>>(num_buckets);
  for (auto bucket = size_t{0}; bucket < num_buckets; ++bucket) {
    const auto bucket_start_index = (bucket == 0) ? 0 : aggregated_bucket_sizes[bucket - 1];
    const auto bucket_block_start = (bucket == 0) ? 0 : aligned_bucket_sizes[bucket - 1];
    DebugAssert(bucket_block_start - bucket_start_index <= std::ranges::size(sort_range), "Size overflow");

    auto bucket_start_begin = std::next(sort_range.begin(), bucket_start_index);
    const auto bucket_start_end = std::next(sort_range.begin(), bucket_block_start);
    bucket_empty_start[bucket] = std::ranges::subrange(bucket_start_begin, bucket_start_end);
  }

  // Create a range of empty elements at the end of each bucket.
  auto bucket_empty_tail = std::vector<std::ranges::subrange<RangeIterator>>(num_buckets);
  for (auto bucket = size_t{0}; bucket < num_buckets; ++bucket) {
    const auto bucket_end_index = static_cast<ssize_t>(aggregated_bucket_sizes[bucket]);
    const auto bucket_block_begin = (bucket > 0) ? static_cast<size_t>(aligned_bucket_sizes[bucket - 1]) : 0;
    auto bucket_written_end_index = write_pointers[bucket].distance_to_start(sort_range);

    if (bucket + 1 == num_buckets && !overflow_bucket.empty()) {
      bucket_written_end_index -= block_size;
    }

    const auto bucket_written_end = std::next(sort_range.begin(), bucket_written_end_index);
    if (total_size <= bucket_block_begin) {
      // Last bucket is empty.
      bucket_empty_tail[bucket] = std::ranges::subrange(bucket_written_end, bucket_written_end);
    } else if (bucket_written_end_index > bucket_end_index) {
      DebugAssert(bucket + 1 == num_buckets, "This case should not happen for the last bucket");
      // Bucket overflows actual size. Because of that we move the overflowing elements to the start of that bucket.
      DebugAssert(bucket_written_end_index <= static_cast<ssize_t>(aligned_bucket_sizes[bucket]),
                  "More blocks written than bucket should contains");
      DebugAssert(bucket_written_end_index - bucket_end_index <= std::ranges::ssize(bucket_empty_start[bucket]),
                  "Unexpected number of bucket elements");
      const auto overflow_begin = std::next(sort_range.begin(), bucket_end_index);
      const auto overflow_end = std::next(sort_range.begin(), bucket_written_end_index);
      DebugAssert(bucket_written_end_index <= static_cast<ssize_t>(total_size), "Overflow stripe size");
      std::copy(overflow_begin, overflow_end, bucket_empty_start[bucket].begin());
      bucket_empty_start[bucket] = cut_range_n(bucket_empty_start[bucket], bucket_written_end_index - bucket_end_index);
      bucket_empty_tail[bucket] = std::ranges::subrange(bucket_written_end, bucket_written_end);
    } else {
      const auto bucket_end = std::next(sort_range.begin(), bucket_end_index);
      bucket_empty_tail[bucket] = std::ranges::subrange(bucket_written_end, bucket_end);
    }
  }
  TRACE_EVENT_END("sort");

  TRACE_EVENT_BEGIN("sort", "ips4o::write_back");
  // Copy all partial blocks from the classification result into the output buffer.
  for (auto bucket = size_t{0}; bucket < num_buckets; ++bucket) {
    for (auto stripe = size_t{0}; stripe < num_stripes; ++stripe) {
      auto& bucket_range = classification_results[stripe].buckets[bucket];
      if (std::ranges::empty(bucket_range)) {
        continue;
      }
      write_to_ranges(bucket_range, bucket_empty_start[bucket], bucket_empty_tail[bucket]);
    }
  }
  if (!overflow_bucket.empty()) {
    write_to_ranges(overflow_bucket, bucket_empty_start.back(), bucket_empty_tail.back());
  }
  TRACE_EVENT_END("sort");

  // Sort each bucket for now.
  TRACE_EVENT_BEGIN("sort", "ips4o::pdqsort");
  const auto sort_task = run_parallel_batched(num_buckets, 1, [&](auto bucket) {
    const auto bucket_start_index = (bucket > 0) ? aggregated_bucket_sizes[bucket - 1] : 0;
    const auto bucket_end_index = aggregated_bucket_sizes[bucket];
    auto bucket_begin = std::next(sort_range.begin(), bucket_start_index);
    auto bucket_end = std::next(sort_range.begin(), bucket_end_index);
    auto range = std::ranges::subrange(bucket_begin, bucket_end);

    ips4o_sort(range, num_buckets, samples_per_classifiers, block_size, min_blocks_per_stripe,
               div_ceil(max_parallelism, 4), comp);
  });
  Hyrise::get().scheduler()->wait_for_tasks(sort_task);
  TRACE_EVENT_END("sort");
}

}  // namespace ips4o

}  // namespace

namespace hyrise {
Sort::Sort(const std::shared_ptr<const AbstractOperator>& input_operator,
           const std::vector<SortColumnDefinition>& sort_definitions, const ChunkOffset output_chunk_size,
           const ForceMaterialization force_materialization, const Config config)
    : AbstractReadOnlyOperator(OperatorType::Sort, input_operator, nullptr,
                               std::make_unique<OperatorPerformanceData<OperatorSteps>>()),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size),
      _force_materialization(force_materialization),
      _config(config) {
  DebugAssert(!_sort_definitions.empty(), "Expected at least one sort criterion");
}

const std ::vector<SortColumnDefinition>& Sort::sort_definitions() const {
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
  return std::make_shared<Sort>(copied_left_input, _sort_definitions, _output_chunk_size, _force_materialization,
                                _config);
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
  // IN PROGRESS
  const auto comp = [normalized_key_size](const NormalizedKeyRow& lhs, const NormalizedKeyRow& rhs) {
    return lhs.less_than(rhs, normalized_key_size);
  };
  ips4o::ips4o_sort(materialized_rows, _config.bucket_count, _config.samples_per_classifier, _config.block_size,
                    _config.min_blocks_per_stripe, _config.max_parallelism, comp);
  /*std::sort(materialized_rows.begin(), materialized_rows.end(),
            [&](const auto& lhs, const auto& rhs) {
              return lhs.less_than(rhs, normalized_key_size);
            });*/

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
                  const std::vector<SortColumnDefinition>& sort_definitions, const Sort::Config& config) {
  for (auto index = size_t{0}; index < 5; ++index) {
    const auto table_wrapper = std::make_shared<TableWrapper>(input_table);
    table_wrapper->execute();
    const auto sort_operator = std::make_shared<Sort>(table_wrapper, sort_definitions, Chunk::DEFAULT_SIZE,
                                                      Sort::ForceMaterialization::No, config);

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

Sort::Config::Config()
    : max_parallelism(std::thread::hardware_concurrency()),
      block_size(256),
      bucket_count(max_parallelism * 2),
      samples_per_classifier(4),
      min_blocks_per_stripe(32) {
  if constexpr (HYRISE_DEBUG) {
    block_size = 4;
    bucket_count = 2;
    samples_per_classifier = 1;
    min_blocks_per_stripe = 4;
  }
}

}  // namespace hyrise
