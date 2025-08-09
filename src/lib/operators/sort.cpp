#include "sort.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <ranges>
#include <string>
#include <thread>
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
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace {

using namespace hyrise;  // NOLINT

constexpr size_t STRING_CUTOFF = 64;

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

// Divide left by right and round up to the next larger integer.
auto div_ceil(const auto left, const auto right) {
  return (left + right - 1) / right;
}

// Returns the next multiple. If value is already a multiple, then return value.
auto next_multiple(const auto value, const auto multiple) {
  return div_ceil(value, multiple) * multiple;
}

size_t opt_batch_size(size_t count, size_t min_batch_size, size_t max_tasks_per_thread) {
  const auto hardware_concurrency = std::thread::hardware_concurrency();
  const auto max_count_per_thread = (count + hardware_concurrency - 1) / hardware_concurrency;
  const auto max_count_per_batch = (max_count_per_thread + max_tasks_per_thread - 1) / max_tasks_per_thread;
  return std::max(max_count_per_batch, min_batch_size);
}

std::vector<std::shared_ptr<AbstractTask>> run_parallel_batched(size_t count, size_t batch_size, auto handler) {
  const auto task_count = (count + batch_size - 1) / batch_size;
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>();
  tasks.reserve(task_count);

  for (auto task_index = size_t{0}; task_index < task_count; ++task_index) {
    const auto begin = task_index * batch_size;
    const auto end = std::min(begin + batch_size, count);
    const auto task = std::make_shared<JobTask>([begin, end, handler] {
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
    const auto column_id = static_cast<ColumnID>(static_cast<uint16_t>(column_index));

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
        const auto output_chunk_id = static_cast<ChunkID>(chunk_index);

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
    // Shortcut: No need to copy RowIDs if input_pos_list is small enough and we do not need to resolve the indirection.
    const auto output_pos_list = std::make_shared<RowIDPosList>(std::move(input_pos_list));
    auto& output_segments = output_segments_by_chunk.at(0);
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      output_segments[column_id] = std::make_shared<ReferenceSegment>(unsorted_table, column_id, output_pos_list);
    }
  } else {
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
    // To keep the implementation simple, we write the output ReferenceSegments in column and chunk pairs. This means
    // that even if input ReferenceSegments share a PosList, the output will contain independent PosLists. While this
    // is slightly more expensive to generate and slightly less efficient for following operators, we assume that the
    // lion's share of the work has been done before the Sort operator is executed and that the relative cost of this
    // is acceptable. In the future, this could be improved.
    auto column_write_tasks = std::vector(column_count, std::vector<std::shared_ptr<AbstractTask>>());
    const auto batch_size = opt_batch_size(output_chunk_count, 8, (2 + column_count - 1) / column_count);
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      column_write_tasks[column_id] = run_parallel_batched(
          output_chunk_count, batch_size,
          [column_id, output_chunk_size, &input_pos_list, &input_segments, &unsorted_table, &output_segments_by_chunk,
           resolve_indirection](const size_t output_chunk_index) {
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

/// Allocate an large chunk of uninitialized memory. This class is faster than C++ std::vector with resize, as it does
/// not initialize each entry.
template <typename T>
class UVector {
 public:
  explicit UVector(const size_t size)
      : _allocator(),
        _ptr(_allocator.allocate(size)),  // NOLINT
        _size(size) {
    Assert(_ptr, "Failed to allocate array");
  }

  UVector(const UVector& other) = delete;
  UVector(UVector&& other) = delete;
  UVector& operator=(const UVector&) = delete;
  UVector& operator=(UVector&&) = delete;

  ~UVector() {
    _allocator.deallocate(_ptr, _size);
  }

  T* begin() {
    return _ptr;
  }

  T* end() {
    return _ptr + _size;
  }

  size_t size() const {
    return _size;
  }

 private:
  std::allocator<T> _allocator;
  T* _ptr;  // NOLINT
  size_t _size;
};

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
  constexpr NormalizedKeyRow() noexcept = default;

  constexpr NormalizedKeyRow(std::byte* key_head, RowID row_id) noexcept : key_head(key_head), row_id(row_id) {}

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
  // Extra bytes required to encode long strings.
  size_t long_width = 0;
  // Contains a null value.
  bool nullable = false;
  // List of long sorted strings.
  std::vector<pmr_string> long_strings;

  // Returns number of bytes required to encode all scanned values. Includes the extra byte for null values.
  size_t width() const {
    return encoding_width + ((long_strings.empty()) ? 0 : long_width) + extra_width + ((nullable) ? 1 : 0);
  }

  ScanResult merge(ScanResult other) const {
    return {
        .encoding_width = encoded_width,
        .extra_width = std::max(extra_width, other.extra_width),
        .long_width = long_width,
        .nullable = nullable || other.nullable,
        .long_strings = std::move(long_strings),
    };
  }
};

/// Scans column for maximum bytes necessary to encode all segment values and null values.
template <typename ColumnDataType>
ScanResult scan_column(const AbstractSegment& segment) {
  auto result = ScanResult();
  segment_with_iterators<ColumnDataType>(segment, [&](auto it, const auto end) {
    while (it != end) {
      const auto& segment_position = *it;
      if (segment_position.is_null()) {
        result.nullable = true;
      } else if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        const auto& value = segment_position.value();
        const auto size = std::min(value.size(), STRING_CUTOFF);
        result.encoding_width = std::max(result.encoding_width, size);
        result.extra_width = sizeof(uint64_t) - (std::countl_zero(result.encoding_width) / size_t{8});

        if (value.size() >= STRING_CUTOFF) {
          result.long_strings.push_back(value);
        }
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

using NormalizedKeyIter = NormalizedKeyRow*;

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
        *(normalized_key_start++) = normalized_null_value;
        // Initialize actual key by setting all bytes to 0.
        for (auto counter = size_t{0}; counter < column_info.width() - 1; ++counter) {
          *(normalized_key_start++) = std::byte{0};
        }
        DebugAssert(normalized_key_start == normalized_key_iter->key_head + offset + column_info.width(),
                    "Encoded unexpected number of bytes");
        ++it, ++normalized_key_iter;
        continue;
      }
      if (column_info.nullable) {
        *(normalized_key_start++) = normalized_non_null_value;
      }

      const auto& value = segment_position.value();

      // Normalize value and encode to byte array.
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        for (const auto chr : value | std::views::take(column_info.encoding_width)) {
          *(normalized_key_start++) = static_cast<std::byte>(chr) ^ modifier;
        }

        // Add null bytes to pad values to correct size. (All keys must have the same size)
        for (auto counter = segment_position.value().size(); counter < column_info.encoding_width; ++counter) {
          *(normalized_key_start++) = std::byte{0} ^ modifier;
        }

        // Encode an identifier for long strings. Use 0 if string is small.
        if (!column_info.long_strings.empty()) {
          if (value.size() < STRING_CUTOFF) {
            copy_uint_to_byte_array(normalized_key_start, uint64_t{0}, column_info.long_width);
          } else {
            const auto iter = std::ranges::lower_bound(column_info.long_strings, value);
            DebugAssert(iter != column_info.long_strings.end(), "Could not find strings");
            const auto index = std::distance(column_info.long_strings.begin(), iter);
            DebugAssert(index >= 0, "Invalid element");
            copy_uint_to_byte_array(normalized_key_start, static_cast<uint64_t>(index + 1), column_info.long_width);
          }
          normalized_key_start += column_info.long_width;
        } else {
          DebugAssert(value.size() <= STRING_CUTOFF, "String must be smaller than the cut off size");
        }

        DebugAssert(column_info.extra_width > 0, "Missing extra width");
        copy_uint_to_byte_array(normalized_key_start, std::min(value.size(), STRING_CUTOFF), column_info.extra_width);
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
        unsigned_value ^= static_cast<UnsignedColumnDataType>(full_modifier);
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

/**
 * A pair of read and write iterators. To allow atomic operations
 */
template <typename It>
class AtomicBlockIteratorPair {
 public:
  // Used for array initialization. Use init to set the initial values.
  AtomicBlockIteratorPair() = default;

  void init(It begin, int64_t write, int64_t read, size_t block_size) {
    DebugAssert(sizeof(size_t) <= 8, "expects at most 64bit for size types");
    DebugAssert(write % block_size == 0, "Write pointer is not block aligned");
    DebugAssert(read % block_size == 0, "Read pointer is not block aligned");
    _block_size = block_size;
    _begin = begin;
    _offset_pair.store(_combine(write, read), std::memory_order_relaxed);
    _pending_reads.store(0, std::memory_order_relaxed);
  }

  /// Decrement read half by one block and return resulting iterator pair. This will also increment the number of
  /// pending reads. Use complete_read to decrement the read.
  std::pair<std::ranges::subrange<It>, std::ranges::subrange<It>> read() {
    _pending_reads.fetch_add(1, std::memory_order_relaxed);
    while (true) {
      auto initial_offset_pair = _offset_pair.load(std::memory_order_relaxed);
      auto [write_offset, read_offset] = _split_offsets(initial_offset_pair);
      const auto new_read_offset = read_offset - _block_size;
      const auto combined = _combine(write_offset, new_read_offset);
      if (_offset_pair.compare_exchange_weak(initial_offset_pair, combined, std::memory_order_acquire,
                                             std::memory_order_relaxed)) {
        const auto write_iter = std::next(_begin, write_offset);
        const auto write_block = std::ranges::subrange(write_iter, std::next(write_iter, _block_size));
        const auto read_iter = std::next(_begin, read_offset);
        const auto read_block = std::ranges::subrange(read_iter, std::next(read_iter, _block_size));
        return {write_block, read_block};
      }
    }
  }

  /// Increment write offset by one block and return resulting read and write iterator pair.
  std::pair<std::ranges::subrange<It>, std::ranges::subrange<It>> write() {
    while (true) {
      auto initial_offset_pair = _offset_pair.load(std::memory_order_relaxed);
      auto [write_offset, read_offset] = _split_offsets(initial_offset_pair);
      const auto new_write_offset = write_offset + _block_size;
      const auto combined = _combine(new_write_offset, read_offset);
      if (_offset_pair.compare_exchange_weak(initial_offset_pair, combined, std::memory_order_relaxed,
                                             std::memory_order_relaxed)) {
        const auto write_iter = std::next(_begin, write_offset);
        const auto write_block = std::ranges::subrange(write_iter, std::next(write_iter, _block_size));
        const auto read_iter = std::next(_begin, read_offset);
        const auto read_block = std::ranges::subrange(read_iter, std::next(read_iter, _block_size));
        return {write_block, read_block};
      }
    }
  }

  /// Distance of write pointer to start of the sort range.
  int64_t distance_to_start(NormalizedKeyRange auto& range) {
    auto [write_offset, _] = _split_offsets(_offset_pair.load(std::memory_order_relaxed));
    return std::distance(range.begin(), _begin) + write_offset;
  }

  /// Checks if some reads are pending.
  bool pending_reads() {
    return _pending_reads.load(std::memory_order_relaxed) != 0;
  }

  /// Decrement the number of pending reads.
  void complete_read() {
    const auto pending_reads = _pending_reads.fetch_sub(1, std::memory_order_relaxed);
    if constexpr (HYRISE_DEBUG) {
      Assert(pending_reads >= 1, "Pending reads shoul be at least one");
    }
  }

 private:
  // Read the atomic value (relaxed) and split the write and read offset (in order).
  static std::pair<int64_t, int64_t> _split_offsets(boost::uint128_type offset_pairs) {
    const auto read_offset = static_cast<int64_t>(offset_pairs);
    const auto write_offset = static_cast<int64_t>(offset_pairs >> uint64_t{64});
    return {write_offset, read_offset};
  }

  // Combine two offset to a single 128-bit unsigned integer.
  static boost::uint128_type _combine(int64_t write_offset, int64_t read_offset) {
    const auto write_64 = static_cast<uint64_t>(write_offset);
    const auto read_64 = static_cast<uint64_t>(read_offset);
    const auto write_128 = static_cast<boost::uint128_type>(write_64);
    const auto read_128 = static_cast<boost::uint128_type>(read_64);
    return (write_128 << uint32_t{64}) | read_128;
  }

  It _begin = It();
  // Uses 128bit value to encode both read and write offset. The upper bits of the value will be the write and the
  // lower the read offset.
  std::atomic<boost::uint128_type> _offset_pair;
  std::atomic<uint32_t> _pending_reads;
  size_t _block_size = 0;
};

// Select the classifiers for the sample sort. At the moment this selects first num classifiers many keys.
std::vector<NormalizedKeyRow> select_classifiers(const NormalizedKeyRange auto& sort_range, size_t num_classifiers,
                                                 size_t samples_per_classifier,
                                                 const NormalizedKeyComparator auto& comp) {
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
  const auto it = std::ranges::lower_bound(classifiers, value, comp);  // NOLINT
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
      std::ranges::move(result.buckets[bucket_index], write_back_begin);
      write_back_begin = std::next(write_back_begin, block_size);
      result.buckets[bucket_index].clear();
      ++result.num_blocks_written;
    }
  }
  return result;
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
  const auto stripe_begin = stripe_range_iter->begin();
  const auto stripe_begin_index = std::distance(sort_range.begin(), stripe_begin);
  const auto stripe_end = stripe_range_iter->end();
  const auto stripe_end_index = std::distance(sort_range.begin(), stripe_end);

  // Find the first bucket ending after the end of this stripe.
  const auto last_bucket_iter = std::ranges::lower_bound(  // NOLINT
      bucket_delimiter, stripe_end_index, [](const int64_t stripe_end, const auto bucket_end) {
        return stripe_end < static_cast<int64_t>(bucket_end);
      });
  const auto last_bucket_index = std::distance(bucket_delimiter.begin(), last_bucket_iter);
  const auto last_bucket_begin =
      (last_bucket_index != 0) ? static_cast<int64_t>(bucket_delimiter[last_bucket_index - 1]) : int64_t{0};
  const auto last_bucket_end = static_cast<int64_t>(bucket_delimiter[last_bucket_index]);
  // Find the stripe the last bucket starts in.
  const auto last_bucket_first_stripe = std::ranges::lower_bound(  // NOLINT
      stripe_ranges, last_bucket_begin, std::less<int64_t>(), [&](const auto& stripe_range) {
        return std::distance(sort_range.begin(), stripe_range.end());
      });
  const auto last_bucket_first_stripe_index = std::distance(stripe_ranges.begin(), last_bucket_first_stripe);

  // Check if the bucket starts in the next stripe. This can happen if the bucket delimiter aligns with the stripe end.
  if (static_cast<int64_t>(stripe_index) < last_bucket_first_stripe_index) {
    return;
  }

  auto num_already_moved_blocks = int64_t{0};
  auto stripe_result_iter = std::next(stripe_results.begin(), last_bucket_first_stripe_index);
  for (auto stripe_iter = last_bucket_first_stripe; stripe_iter != stripe_range_iter;
       ++stripe_iter, ++stripe_result_iter) {
    const auto stripe_begin = std::distance(sort_range.begin(), stripe_iter->begin());
    const auto stripe_end = std::distance(sort_range.begin(), stripe_iter->end());
    const auto written_end = static_cast<int64_t>(stripe_begin + (stripe_result_iter->num_blocks_written * block_size));
    const auto empty_begin = std::max(written_end, last_bucket_begin);
    const auto num_empty_blocks = (stripe_end - empty_begin) / static_cast<int64_t>(block_size);
    DebugAssert(num_empty_blocks >= 0, "Expects a positive number of empty blocks.");

    // At these empty to blocks to the number of already processed blocks, because they are processed by different
    // threads.
    num_already_moved_blocks += num_empty_blocks;
  }

  // Calculate the number of blocks which must be filled by this stripe.
  const auto written_end = static_cast<int64_t>(stripe_begin_index + (stripe_result->num_blocks_written * block_size));
  const auto empty_begin = std::max(written_end, last_bucket_begin);
  DebugAssert(empty_begin <= stripe_end_index, "Empty region ends after");
  const auto num_empty_blocks = static_cast<int64_t>((stripe_end_index - empty_begin) / block_size);

  auto move_num_blocks = num_empty_blocks;
  const auto stripe_empty_begin_index = stripe_begin_index + (stripe_result->num_blocks_written * block_size);
  const auto bucket_empty_begin = std::max(last_bucket_begin, static_cast<int64_t>(stripe_empty_begin_index));
  auto stripe_empty_begin = std::next(sort_range.begin(), bucket_empty_begin);

  // Find last stripe a bucket is in.
  auto last_stripe = std::ranges::lower_bound(  // NOLINT
      stripe_ranges, last_bucket_end, std::less<int64_t>(), [&](const auto& stripe_range) {
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
    auto num_moveable_blocks = static_cast<int64_t>(std::min(bucket_num_blocks, num_blocks_written));

    if (num_already_moved_blocks >= num_moveable_blocks) {
      num_already_moved_blocks -= num_moveable_blocks;
      continue;
    }
    num_moveable_blocks -= num_already_moved_blocks;
    const auto blocks_to_move = std::min(num_moveable_blocks, move_num_blocks);

    auto stripe_full_begin = std::next(last_stripe->begin(), (num_moveable_blocks - 1) * block_size);
    for (auto counter = int64_t{0}; counter < blocks_to_move; ++counter) {
      const auto block_end = std::next(stripe_full_begin, block_size);
      std::move(stripe_full_begin, block_end, stripe_empty_begin);
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
void permute_blocks(const NormalizedKeyRange auto& classifiers, const size_t stripe, auto& block_iterators,
                    const NormalizedKeyComparator auto& comp, size_t num_buckets, size_t block_size,
                    std::vector<NormalizedKeyRow>& overflow_bucket, const auto overflow_bucket_begin) {
  auto target_buffer = std::vector<NormalizedKeyRow>(block_size);
  auto swap_buffer = std::vector<NormalizedKeyRow>(block_size);

  // Cycle through each bucket once.
  for (auto counter = size_t{0}, bucket = stripe % num_buckets; counter < num_buckets;
       ++counter, bucket = (bucket + 1) % num_buckets) {
    while (true) {
      auto [write_block, read_block] = block_iterators[bucket].read();
      if (std::distance(write_block.begin(), read_block.begin()) < 0) {
        block_iterators[bucket].complete_read();
        // Read iterator is before writer iterator: It follows, that no new blocks are left to read. We continue read
        // with the next bucket.
        break;
      }
      std::ranges::move(read_block, target_buffer.begin());
      block_iterators[bucket].complete_read();

      auto target_bucket = classify_value(target_buffer[0], classifiers, comp);
      while (true) {
        auto [write_block, read_block] = block_iterators[target_bucket].write();
        if (std::distance(write_block.begin(), read_block.begin()) < 0) {
          /// Wait until all reads are complete.
          while (block_iterators[target_bucket].pending_reads()) {}

          if (write_block.begin() == overflow_bucket_begin) {
            // Special Case: Let assume we have the following array to sort: [a a b c c]. In addition, we assume that
            // the bucket size is 2. A array with delimiter may look like [a a|b _|c c], but this is array is one
            // element longer than the original array. Because of that we provide an overflow bucket, which can be
            // written to in this case.
            overflow_bucket.resize(block_size);
            std::ranges::move(target_buffer, overflow_bucket.begin());
          } else {
            std::ranges::move(target_buffer, write_block.begin());
          }
          break;
        }

        auto swap_bucket = classify_value(*write_block.begin(), classifiers, comp);
        if (swap_bucket == target_bucket) {
          // Write block is already in the correct block. Skip this block and write to the next one.
          continue;
        }

        std::ranges::move(write_block, swap_buffer.begin());
        std::ranges::move(target_buffer, write_block.begin());
        std::swap(target_buffer, swap_buffer);
        target_bucket = swap_bucket;
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
 * Do a single pass of IPS4o and return an array of bucket delimiter. This past will first classify the input array
 * into blocks. Therefore, the algorithm relies on a sample sort with the provided number of buckets. In a second
 * stage this algorithm will move the block into the correct buckets and finally, it will cleanup the bucket border
 * and add missing elements.
 *
 * @param sort_range             Range of values to sort.
 * @param num_buckets            Number of buckets to classify the elements into. At least to buckets are required.
 * @param samples_per_classifier Number of samples per classifier to select from the array.
 * @param block_size             Number of array elements per block.
 * @param num_stripes            Number of stripes to distribute blocks on. This is equivalent to the maximum amount of
 *								 parallelism.
 * @param comp                   Function for comparing a < b.
 *
 * @return Returns the size of each bucket.
 */
std::vector<size_t> ips4o_pass(NormalizedKeyRange auto& sort_range, const size_t num_buckets,
                               const size_t samples_per_classifier, const size_t block_size, size_t num_stripes,
                               const NormalizedKeyComparator auto& comp) {
  Assert(num_buckets > 1, "At least two buckets are required");
  Assert(num_stripes > 1, "This function should be called with at least two 2 stripes. Use pdqsort instead");
  Assert(std::ranges::size(sort_range) >= block_size, "Provide at least one block");

  using RangeIterator = decltype(std::ranges::begin(sort_range));

  // The following terms are used in this algorithm:
  // - *block* A number of sequential elements. The sort_range is split up into these blocks.
  // - *stripe* A stripe is a set of sequential blocks. Each stripe is executed in its own task/thread.

  DebugAssert(block_size > 0, "A block must contains at least one element.");
  const auto total_size = std::ranges::size(sort_range);
  const auto num_blocks = div_ceil(total_size, block_size);
  const auto num_classifiers = num_buckets - 1;
  // Calculate the number of blocks assigned to each stripe.
  const auto max_blocks_per_stripe = div_ceil(num_blocks, num_stripes);

  DebugAssert(num_blocks > 0, "At least one block is required.");
  DebugAssert(num_buckets > 0, "At least one bucket is required.");

  // Select the elements for the sample sort.
  const auto classifiers = select_classifiers(sort_range, num_classifiers, samples_per_classifier, comp);

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
  auto classification_results = std::vector<StripeClassificationResult>(num_stripes);
  const auto classification_tasks = run_parallel_batched(num_stripes, 1, [&](auto stripe) {
    classification_results[stripe] = classify_stripe(stripes[stripe], classifiers, block_size, comp);
  });
  Hyrise::get().scheduler()->wait_for_tasks(classification_tasks);

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

  const auto prepare_tasks = run_parallel_batched(num_stripes, 1, [&](auto stripe) {
    prepare_block_permutations(sort_range, stripe, stripes, classification_results, aligned_bucket_sizes, block_size);
  });
  Hyrise::get().scheduler()->wait_for_tasks(prepare_tasks);

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
  auto bucket_iterators = std::vector<AtomicBlockIteratorPair<RangeIterator>>(num_buckets);
  auto bucket_begin = sort_range.begin();
  for (auto bucket = size_t{0}, stripe = size_t{0}; bucket < num_buckets; ++bucket) {
    const auto delimiter_begin = std::distance(sort_range.begin(), bucket_begin);
    DebugAssert(delimiter_begin % block_size == 0, "Delimiter is misaligned");
    const auto delimiter_end = static_cast<int64_t>(aligned_bucket_sizes[bucket]);
    DebugAssert(delimiter_end % block_size == 0, "Delimiter is misaligned");

    // Sum the number of blocks written to this bucket.
    auto num_blocks = int64_t{0};
    while (stripe < num_stripes) {
      const auto stripe_begin = std::distance(sort_range.begin(), stripes[stripe].begin());
      DebugAssert(stripe_begin % block_size == 0, "Stripe begin is misaligned");
      const auto stripe_end = std::distance(sort_range.begin(), stripes[stripe].end());
      const auto stripe_written_end =
          static_cast<int64_t>(stripe_begin + (block_size * classification_results[stripe].num_blocks_written));
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
    const auto read_pointer_offset = (num_blocks - 1) * static_cast<int64_t>(block_size);

    bucket_iterators[bucket].init(bucket_begin, 0, read_pointer_offset, block_size);
    bucket_begin = std::next(sort_range.begin(), delimiter_end);
  }

  const auto overflow_bucket_offset = (std::ranges::size(sort_range) / block_size) * block_size;
  const auto overflow_bucket_begin = std::next(sort_range.begin(), overflow_bucket_offset);
  auto overflow_bucket = std::vector<NormalizedKeyRow>();

  // Move blocks into the correct bucket (based on the aligned bucket delimiter).
  const auto permute_blocks_tasks = run_parallel_batched(num_stripes, 1, [&](auto stripe) {
    permute_blocks(classifiers, stripe, bucket_iterators, comp, num_buckets, block_size, overflow_bucket,
                   overflow_bucket_begin);
  });
  Hyrise::get().scheduler()->wait_for_tasks(permute_blocks_tasks);

  // Until this point all buckets are aligned to the block_size and there still some classified elements in the stripe
  // local buffer left. Because of that, we know move elements overflowing the bucket to the start of that bucket and
  // copy the values from the stripe classification result to empty elements at the buffer begin/end.

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
    const auto bucket_end_index = static_cast<int64_t>(aggregated_bucket_sizes[bucket]);
    const auto bucket_block_begin = (bucket > 0) ? static_cast<size_t>(aligned_bucket_sizes[bucket - 1]) : 0;
    auto bucket_written_end_index = bucket_iterators[bucket].distance_to_start(sort_range);

    if (bucket + 1 == num_buckets && !overflow_bucket.empty()) {
      bucket_written_end_index -= block_size;
    }

    const auto bucket_written_end = std::next(sort_range.begin(), bucket_written_end_index);
    if (total_size <= bucket_block_begin) {
      // Last bucket is empty.
      bucket_empty_tail[bucket] = std::ranges::subrange(bucket_written_end, bucket_written_end);
    } else if (bucket_written_end_index > bucket_end_index) {
      DebugAssert(bucket + 1 != num_buckets, "This case should not happen for the last bucket");
      // Bucket overflows actual size. Because of that we move the overflowing elements to the start of that bucket.
      DebugAssert(bucket_written_end_index <= static_cast<int64_t>(aligned_bucket_sizes[bucket]),
                  "More blocks written than bucket should contains");
      DebugAssert(bucket_written_end_index - bucket_end_index <= std::ranges::ssize(bucket_empty_start[bucket]),
                  "Unexpected number of bucket elements");
      const auto overflow_begin = std::next(sort_range.begin(), bucket_end_index);
      const auto overflow_end = std::next(sort_range.begin(), bucket_written_end_index);
      DebugAssert(bucket_written_end_index <= static_cast<int64_t>(total_size), "Overflow stripe size");
      std::move(overflow_begin, overflow_end, bucket_empty_start[bucket].begin());
      bucket_empty_start[bucket] = cut_range_n(bucket_empty_start[bucket], bucket_written_end_index - bucket_end_index);
      bucket_empty_tail[bucket] = std::ranges::subrange(bucket_written_end, bucket_written_end);
    } else {
      const auto bucket_end = std::next(sort_range.begin(), bucket_end_index);
      bucket_empty_tail[bucket] = std::ranges::subrange(bucket_written_end, bucket_end);
    }
  }

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

  if constexpr (HYRISE_DEBUG) {
    for (auto bucket = size_t{0}; bucket < num_buckets; ++bucket) {
      DebugAssert(std::ranges::empty(bucket_empty_start[bucket]),
                  std::format("Bucket {} is not fully filled (start)", bucket));
      DebugAssert(std::ranges::empty(bucket_empty_tail[bucket]),
                  std::format("Bucket {} is not fully filled (tail)", bucket));
    }
  }

  return aggregated_bucket_sizes;
}

/**
 * This sorting algorithm consists of two sorting strategies:
 *
 * 1. A single pass of IPS4o to divide the input array into smaller buckets, and
 * 2. pdqsort to sort small enough buckets.
 *
 * A bucket is small enough, if it contains not enough elements to create at least two stripes (See config.min_batch_size).
 */
void sort(NormalizedKeyRange auto& sort_range, const Sort::Config& config, const NormalizedKeyComparator auto& comp) {
  using RangeIterator = decltype(std::ranges::begin(sort_range));

  const auto total_size = std::ranges::size(sort_range);
  const auto total_num_blocks = div_ceil(total_size, config.block_size);
  if (total_num_blocks < 2 * config.min_blocks_per_stripe) {
    boost::sort::pdqsort(sort_range.begin(), sort_range.end(), comp);
    return;
  }

  // Do an initial pass of IPS4o.
  const auto num_stripes = std::min(total_num_blocks / config.min_blocks_per_stripe, config.max_parallelism);
  auto bucket_lock = std::mutex();
  auto buckets = std::vector<std::ranges::subrange<RangeIterator>>();
  buckets.reserve(config.bucket_count * config.bucket_count);
  buckets.emplace_back(sort_range.begin(), sort_range.end());

  auto unsplittable_buckets = std::vector<std::ranges::subrange<RangeIterator>>();
  unsplittable_buckets.reserve(config.bucket_count);

  auto unparsed_buckets = std::ranges::subrange(buckets.begin(), buckets.end());
  const auto small_bucket_max_blocks = div_ceil(total_num_blocks, config.bucket_count);
  const auto small_bucket_max_size =
      std::max(small_bucket_max_blocks * config.block_size, config.max_parallelism * config.block_size);
  for (auto round = size_t{0}; round < 4; ++round) {
    auto large_bucket_range = std::ranges::partition(unparsed_buckets, [&](const auto& range) {
      return std::ranges::size(range) <= small_bucket_max_size;
    });
    auto large_buckets = std::vector(large_bucket_range.begin(), large_bucket_range.end());
    buckets.erase(large_bucket_range.begin(), large_bucket_range.end());
    if (large_buckets.size() == 0) {
      break;
    }

    const auto small_offset = buckets.size();
    const auto round_tasks = run_parallel_batched(large_buckets.size(), 1, [&](const auto bucket) {
      const auto large_range = large_buckets[bucket];
      DebugAssert(std::ranges::size(large_range) >= small_bucket_max_size, "Large bucket");

      const auto delimiter = ips4o_pass(large_range, config.bucket_count, config.samples_per_classifier,
                                        config.block_size, num_stripes, comp);

      const auto guard = std::lock_guard(bucket_lock);
      auto bucket_begin_index = size_t{0};
      for (const auto bucket_delimiter : delimiter) {
        const auto begin = std::next(large_range.begin(), bucket_begin_index);
        const auto end = std::next(large_range.begin(), bucket_delimiter);
        const auto bucket_size = bucket_delimiter - bucket_begin_index;
        if (bucket_size == std::ranges::size(large_range)) {
          unsplittable_buckets.emplace_back(begin, end);
        } else if (bucket_size > 0) {
          buckets.emplace_back(begin, end);
        }
        bucket_begin_index = bucket_delimiter;
      }
    });
    Hyrise::get().scheduler()->wait_for_tasks(round_tasks);
    const auto small_begin = std::next(buckets.begin(), small_offset);
    unparsed_buckets = std::ranges::subrange(small_begin, buckets.end());
  }

  // Sort buckets by size to first sort large buckets.
  boost::sort::pdqsort(buckets.begin(), buckets.end(), [](const auto& left, const auto& right) {
    return std::ranges::size(left) > std::ranges::size(right);
  });

  const auto unsplittable_buckets_tasks = run_parallel_batched(unsplittable_buckets.size(), 1, [&](const auto bucket) {
    boost::sort::pdqsort(unsplittable_buckets[bucket].begin(), unsplittable_buckets[bucket].end(), comp);
  });
  const auto sort_tasks = run_parallel_batched(buckets.size(), 1, [&](const auto bucket) {
    boost::sort::pdqsort(buckets[bucket].begin(), buckets[bucket].end(), comp);
  });

  Hyrise::get().scheduler()->wait_for_tasks(unsplittable_buckets_tasks);
  Hyrise::get().scheduler()->wait_for_tasks(sort_tasks);
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
  auto timer = Timer{};
  const auto& input_table = left_input_table();

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

  // Scan all chunks for the maximum number of bytes necessary to represent all column values. The scanning is
  // done in parallel on multiple threads.

  const auto search_column_count = _sort_definitions.size();
  const auto chunk_count = input_table->chunk_count();

  auto chunk_stats = std::vector<std::vector<ScanResult>>(search_column_count, std::vector<ScanResult>(chunk_count));

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
          chunk_stats[column_index][chunk_id].encoding_width = sizeof(ColumnDataType);
          chunk_stats[column_index][chunk_id].nullable = column_is_nullable;
        }
      } else {
        // Elements may have different size.
        tasks_per_column[column_index] = run_parallel_batched(
            chunk_count, batch_size, [column_id, column_index, &chunk_stats, &input_table](const auto chunk_index) {
              const auto chunk_id = static_cast<ChunkID>(chunk_index);
              const auto segment = input_table->get_chunk(chunk_id)->get_segment(column_id);
              chunk_stats[column_index][chunk_id] = scan_column<ColumnDataType>(*segment);
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
    auto aggregated_stats = std::accumulate(chunk_stats[column_index].begin(), chunk_stats[column_index].end(),
                                            ScanResult{}, [](ScanResult result, ScanResult chunk) {
                                              return result.merge(chunk);
                                            });
    Assert(aggregated_stats.width() > 0, std::format("Invalid width for column {}", static_cast<uint16_t>(column_id)));
    normalized_key_size += aggregated_stats.width();
    column_stats[column_index] = std::move(aggregated_stats);
  }

  const auto sort_long_strings = run_parallel_batched(search_column_count, 1, [&](const auto column) {
    boost::sort::pdqsort(column_stats[column].long_strings.begin(), column_stats[column].long_strings.end());
  });
  Hyrise::get().scheduler()->wait_for_tasks(sort_long_strings);

  const auto padded_key_size = ((normalized_key_size + 3) / 4) * 4;

  // const auto scan_time = timer.lap();
  timer.lap();

  // Convert the columnar layout into a row layout for better sorting. This is done by encoding all sorted columns
  // into an array of bytes. These rows can be compared using memcmp.

  // UVector is used instead of std::vector, because std::vector.resize() not only allocates the necessary memory but
  // also initializes all elements. This is not necessary, as we initialize all elements in parallel as part of the
  // next step. The performance improves by about 10%.
  auto materialized_rows = UVector<NormalizedKeyRow>(input_table->row_count());

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
    const auto chunk = input_table->get_chunk(chunk_id);
    const auto chunk_size = chunk->size();
    chunk_allocations[chunk_id] = pmr_vector<std::byte>(chunk_size * padded_key_size);

    auto normalized_key_iter = materialized_rows.begin() + chunk_offsets[chunk_index];
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      const auto row_id = RowID{chunk_id, chunk_offset};
      *normalized_key_iter++ =
          NormalizedKeyRow(chunk_allocations[chunk_id].data() + (chunk_offset * padded_key_size), row_id);
    }

    normalized_key_iter = materialized_rows.begin() + chunk_offsets[chunk_index];
    auto key_offset = size_t{0};
    for (auto index = size_t{0}; index < search_column_count; ++index) {
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

  const auto materialization_time = timer.lap();

  // IN PROGRESS
  const auto comp = [normalized_key_size](const NormalizedKeyRow& lhs, const NormalizedKeyRow& rhs) {
    return lhs.less_than(rhs, normalized_key_size);
  };
  ips4o::sort(materialized_rows, _config, comp);

  const auto sort_time = timer.lap();

  // Extract the positions from the sorted rows.
  auto position_list = RowIDPosList();
  position_list.reserve(materialized_rows.size());
  for (const auto& row : materialized_rows) {
    position_list.push_back(row.row_id);
  }

  const auto write_back_time = timer.lap();

  // TODO(student): Update performance metrics.
  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  step_performance_data.set_step_runtime(OperatorSteps::MaterializeSortColumns, materialization_time);
  step_performance_data.set_step_runtime(OperatorSteps::TemporaryResultWriting, write_back_time);
  step_performance_data.set_step_runtime(OperatorSteps::Sort, sort_time);

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
