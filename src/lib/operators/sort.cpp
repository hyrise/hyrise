#include "sort.hpp"

#include <algorithm>
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
#include "utils/ips4o_sort.hpp"
#include "utils/timer.hpp"

namespace {

using namespace hyrise;  // NOLINT

// The number of bytes after that long strings are shortened.
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
  Assert(pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList.");

  // Materialize column by column, starting a new ValueSegment whenever output_chunk_size is reached
  const auto input_chunk_count = unsorted_table->chunk_count();
  const auto output_column_count = unsorted_table->column_count();
  const auto row_count = unsorted_table->row_count();

  // Vector of segments for each chunk
  auto output_segments_by_chunk = std::vector(output_chunk_count, Segments(output_column_count));

  auto materialize_tasks = std::vector<std::shared_ptr<AbstractTask>>();
  materialize_tasks.reserve(output_column_count * output_chunk_count);
  for (auto column_id = ColumnID{0}; column_id < output_column_count; ++column_id) {
    const auto column_data_type = output->column_data_type(column_id);
    const auto column_is_nullable = unsorted_table->column_is_nullable(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto accessor_by_chunk_id =
          std::make_shared<std::vector<std::unique_ptr<AbstractSegmentAccessor<ColumnDataType>>>>(input_chunk_count);
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto chunk = unsorted_table->get_chunk(input_chunk_id);
        Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk #1686.");
        const auto abstract_segment = chunk->get_segment(column_id);
        DebugAssert(abstract_segment, "Segment should exist.");
        (*accessor_by_chunk_id)[input_chunk_id] = create_segment_accessor<ColumnDataType>(abstract_segment);
        DebugAssert((*accessor_by_chunk_id)[input_chunk_id], "Accessor must be initialized.");
      }

      for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
        materialize_tasks.emplace_back(std::make_shared<JobTask>([&, column_id, output_chunk_id, output_chunk_size,
                                                                  row_count, column_is_nullable,
                                                                  accessor_by_chunk_id]() {
          const auto output_segment_begin = output_chunk_id * output_chunk_size;
          const auto output_segment_end =
              std::min(output_segment_begin + output_chunk_size, static_cast<uint32_t>(row_count));
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
            DebugAssert((*accessor_by_chunk_id)[input_chunk_id], "ChunkID must be valid.");
            auto value = (*accessor_by_chunk_id)[input_chunk_id]->access(input_chunk_offset);
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
        }));
      }
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(materialize_tasks);

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
  Assert(input_pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList.");

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
        DebugAssert(input_segments[column_id][chunk_id], "Expected valid segment.");
      }
    }
    // To keep the implementation simple, we write the output ReferenceSegments in column and chunk pairs. This means
    // that even if input ReferenceSegments share a PosList, the output will contain independent PosLists. While this
    // is slightly more expensive to generate and slightly less efficient for following operators, we assume that the
    // lion's share of the work has been done before the Sort operator is executed and that the relative cost of this
    // is acceptable. In the future, this could be improved.

    auto output_write_column_tasks = std::vector<std::shared_ptr<AbstractTask>>();
    output_write_column_tasks.reserve(column_count * output_chunk_count);
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
        output_write_column_tasks.emplace_back(std::make_shared<JobTask>([&, column_id, output_chunk_id]() {
          auto input_pos_list_offset = static_cast<size_t>(output_chunk_id) * output_chunk_size;
          auto input_pos_list_end = std::min(input_pos_list_offset + output_chunk_size, input_pos_list.size());

          const auto first_reference_segment =
              std::dynamic_pointer_cast<ReferenceSegment>(input_segments[column_id][output_chunk_id]);
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
                          "Input column references more than one table.");
              DebugAssert(input_reference_segment.referenced_column_id() == referenced_column_id,
                          "Input column references more than one column.");
              const auto& input_reference_pos_list = input_reference_segment.pos_list();
              output_pos_list->emplace_back((*input_reference_pos_list)[row_id.chunk_offset]);
            } else {
              output_pos_list->emplace_back(row_id);
            }
          }

          output_segments_by_chunk[output_chunk_id][column_id] =
              std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, output_pos_list);
        }));
      }
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(output_write_column_tasks);
  }

  for (auto& segments : output_segments_by_chunk) {
    output_table->append_chunk(segments);
  }

  return output_table;
}

// Allocate a large chunk of uninitialized memory. This class is faster than C++ std::vector with resize, as it does
/// not initialize each entry.
template <typename T>
class UVector {
 public:
  explicit UVector(const size_t size) : _allocator(), _ptr(_allocator.allocate(size)), _size(size) {
    Assert(_ptr, "Failed to allocate array.");
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
  T* _ptr;
  size_t _size;
};

/**
 * Implementation of DuckDB's static memcmp. This optimizes calls to memcmp by passing a compile-time known length. If
 * optimized correctly the compiler would resolve the implementation to a switch like statement.
 *
 * `start` and `end` define the range of compile-time known lengthes.
 */
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
    DebugAssert(key_head, "NormalizedKey is not valid.");
    DebugAssert(other.key_head, "NormalizedKey is not valid.");
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

  ScanResult merge(ScanResult& other) {
    const auto old_size = long_strings.size();
    long_strings.resize(old_size + other.long_strings.size());
    auto begin = std::next(long_strings.begin(), static_cast<int64_t>(old_size));
    std::ranges::move(other.long_strings, begin);
    other.long_strings.clear();
    // Calculate number of bytes required to encode long string offsets. Also account for the extra element to encode
    // shorter strings.
    const auto encoded_width = std::max(encoding_width, other.encoding_width);
    const auto long_width = sizeof(uint64_t) - (std::countl_zero(long_strings.size() + 1) / size_t{8});
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
          static_assert(std::is_unsigned_v<UnsignedColumnDataType>, "Can not convert column data type to unsigned.");
          // We often encounter small number, but we still 32-bit numbers while 8 or 16 bit would be sufficient.
          // Because of that we want to reduce the bit-width of our integer numbers to the bare minimum. The idea
          // is to drop all leading 0 for positive integers and all leading ones for negative numbers. We only keep
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

// Copy an integer to the byte array. To ensure the encoded integer is comparable with memcmp, the endianess of the
// encoded integer is changed to big-endian. In addition, it will take the number of bytes necessary to encode this
// integer, because of the variable length encoding of int32_t and uint32_t (see materialization and scanning).
void copy_uint_to_byte_array(std::byte* byte_array, std::unsigned_integral auto uint,
                             size_t len = sizeof(decltype(uint))) {
  if constexpr (std::endian::native == std::endian::little) {
    uint = byteswap(uint);
  } else if constexpr (std::endian::native != std::endian::big) {
    Fail("Mixed-endian is unsupported.");
  }
  const auto* uint_byte_array = reinterpret_cast<std::byte*>(&uint);
  memcpy(byte_array, uint_byte_array + (sizeof(decltype(uint)) - len), len);
}

using NormalizedKeyIter = NormalizedKeyRow*;

// Append segment values as byte array to the normalized keys. Expects that the normalized_key_iter is valid for
// each segment's values.
//
// Parameters:
// @param segment              Segment to encode values from
// @param offset               Offset to the start of the normalized key row's byte array.
// @param expected_width       The number of bytes all values must be encoded to.
// @param ascending            Sort normalized keys in ascending order.
// @param nullable             Put extra byte in front to encode null values.
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
  segment_with_iterators<ColumnDataType>(segment, [&](auto it, const auto& end) {
    while (it != end) {
      const auto segment_position = *it;
      DebugAssert(column_info.nullable || !segment_position.is_null(),
                  "Segment must be nullable or contains no null values.");
      auto* normalized_key_start = normalized_key_iter->key_head + offset;

      // Encode the null byte for nullable segments. This byte is used to order null vs. non-null values (e.g.
      // put them to the front or end depending on the specified null order).
      if (column_info.nullable && segment_position.is_null()) {
        *(normalized_key_start++) = normalized_null_value;
        // Initialize actual key by setting all bytes to 0.
        for (auto counter = size_t{0}; counter < column_info.width() - 1; ++counter) {
          *(normalized_key_start++) = std::byte{0};
        }
        DebugAssert(normalized_key_start == normalized_key_iter->key_head + offset + column_info.width(),
                    "Encoded unexpected number of bytes.");
        ++it, ++normalized_key_iter;
        continue;
      }
      if (column_info.nullable) {
        *(normalized_key_start++) = normalized_non_null_value;
      }

      const auto& value = segment_position.value();

      // Normalize value and encode to byte array.
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        // String values are encoded by copying all bytes of the string to the normalized key and filling the remaing
        // bytes with zero values. Because we want to avoid padding for very long strings, we cut of the long string
        // and replace their tail with a offset into an presorted array. In addition, we also need to account for
        // null bytes in strings. Therefore, the length of the strings is appended in the end. The resulting
        // representation looks as follows:
        //
        // String representation: of "Hello, World!" and the longest string has 16 bytes.
        //
        // | Encoded String                                  | Long String Offset | Length |
        // | 48 65 6c 6c 6f 2c 20 57 6f 72 6c 6f 21 00 00 00 | 00                 | 0d     |
        //
        // Now lets assume the long string cut-off is after 8 bytes and "Hello, World!" is the first of all long
        // strings:
        //
        // | Encoded String          | Long String Offset | Length |
        // | 48 65 6c 6c 6f 2c 20 57 | 01                 | 08     |
        //
        // We can see two things:
        //
        // - The long string offset is indexed with 1, because 0 reserved for all shorter strings, and
        // - the length is capped to the encoded string length

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
            const auto iter = std::ranges::lower_bound(column_info.long_strings, value);  // NOLINT
            DebugAssert(iter != column_info.long_strings.end(), "Could not find strings.");
            const auto index = std::distance(column_info.long_strings.begin(), iter);
            DebugAssert(index >= 0, "Invalid element.");
            copy_uint_to_byte_array(normalized_key_start, static_cast<uint64_t>(index + 1), column_info.long_width);
          }
          normalized_key_start += column_info.long_width;
        } else {
          DebugAssert(value.size() <= STRING_CUTOFF, "String must be smaller than the cut off size.");
        }

        DebugAssert(column_info.extra_width > 0, "Missing extra width.");
        copy_uint_to_byte_array(normalized_key_start, std::min(value.size(), STRING_CUTOFF), column_info.extra_width);
        normalized_key_start += column_info.extra_width;
      } else if constexpr (std::is_same_v<ColumnDataType, int32_t> || std::is_same_v<ColumnDataType, int64_t>) {
        using UnsignedColumnDataType = decltype(to_unsigned(ColumnDataType{}));
        static_assert(std::is_unsigned_v<UnsignedColumnDataType>, "Can not convert column data type to unsigned.");

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
        static_assert(std::is_unsigned_v<UnsignedColumnDataType>, "Can not convert column data type to unsigned.");

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
        Fail(std::format("Cannot encode values of type '{}'.", typeid(ColumnDataType).name()));
      }
      DebugAssert(normalized_key_start == normalized_key_iter->key_head + offset + column_info.width(),
                  "Encoded unexpected number of bytes.");
      ++it, ++normalized_key_iter;
    }
  });
}

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
  DebugAssert(!_sort_definitions.empty(), "Expected at least one sort criterion.");
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
           "Sort: Column ID is greater than table's column count.");
  }

  if (input_table->row_count() == 0) {
    if (_force_materialization == ForceMaterialization::Yes && input_table->type() == TableType::References) {
      return Table::create_dummy_table(input_table->column_definitions());
    }
    return input_table;
  }

  auto sorted_table = std::shared_ptr<Table>{};

  // const auto init_time = timer.lap();
  timer.lap();

  // Scan all chunks for the maximum number of bytes necessary to represent all column values. The scanning is
  // done in parallel on multiple threads.

  const auto search_column_count = _sort_definitions.size();
  const auto chunk_count = input_table->chunk_count();

  auto chunk_stats = std::vector<std::vector<ScanResult>>(search_column_count, std::vector<ScanResult>(chunk_count));

  auto contains_string_column = false;
  auto scan_tasks = std::vector<std::shared_ptr<AbstractTask>>();
  scan_tasks.reserve(search_column_count * chunk_count);

  for (auto column_index = size_t{0}; column_index < search_column_count; ++column_index) {
    const auto column_id = _sort_definitions[column_index].column;
    const auto column_data_type = input_table->column_data_type(column_id);
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
        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          scan_tasks.emplace_back(std::make_shared<JobTask>([&, column_id, column_index, chunk_id]() {
            const auto segment = input_table->get_chunk(chunk_id)->get_segment(column_id);
            chunk_stats[column_index][chunk_id] = scan_column<ColumnDataType>(*segment);
          }));
        }
      }
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(scan_tasks);

  auto column_stats = std::vector<ScanResult>(search_column_count, ScanResult{});
  auto normalized_key_size = size_t{0};
  for (auto column_index = size_t{0}; column_index < search_column_count; ++column_index) {
    const auto column_id = _sort_definitions[column_index].column;
    auto aggregated_stats = std::accumulate(chunk_stats[column_index].begin(), chunk_stats[column_index].end(),
                                            ScanResult{}, [](ScanResult result, ScanResult& chunk) {
                                              return result.merge(chunk);
                                            });
    Assert(aggregated_stats.width() > 0, std::format("Invalid width for column {}.", static_cast<uint16_t>(column_id)));
    normalized_key_size += aggregated_stats.width();
    column_stats[column_index] = std::move(aggregated_stats);
  }

  auto sort_long_string_tasks = std::vector<std::shared_ptr<AbstractTask>>();
  sort_long_string_tasks.reserve(search_column_count);
  for (auto column_id = ColumnID{0}; column_id < search_column_count; ++column_id) {
    // Only spawn task for sorting if their is actually something to sort.
    if (column_stats[column_id].long_strings.size() > 1) {
      sort_long_string_tasks.emplace_back(std::make_shared<JobTask>([column_id, &column_stats]() {
        auto& long_strings = column_stats[column_id].long_strings;
        boost::sort::pdqsort(long_strings.begin(), long_strings.end());
      }));
    }
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(sort_long_string_tasks);

  const auto padded_key_size = ((normalized_key_size + 3) / 4) * 4;

  // const auto scan_time = timer.lap();
  timer.lap();

  // Convert the columnar layout into a row layout for better sorting. This is done by encoding all sorted columns
  // into an array of bytes. These rows can be compared using memcmp.

  // UVector is used instead of std::vector, because std:uvector.resize() not only allocates the necessary memory but
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
  auto materialization_tasks = std::vector<std::shared_ptr<AbstractTask>>();
  materialization_tasks.reserve(chunk_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    materialization_tasks.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      const auto chunk = input_table->get_chunk(chunk_id);
      const auto chunk_size = chunk->size();
      chunk_allocations[chunk_id] = pmr_vector<std::byte>(chunk_size * padded_key_size);

      auto* normalized_key_iter = materialized_rows.begin() + chunk_offsets[chunk_id];
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
        const auto row_id = RowID{chunk_id, chunk_offset};
        *normalized_key_iter++ =
            NormalizedKeyRow(chunk_allocations[chunk_id].data() + (chunk_offset * padded_key_size), row_id);
      }

      normalized_key_iter = materialized_rows.begin() + chunk_offsets[chunk_id];
      auto key_offset = size_t{0};
      for (auto index = size_t{0}; index < search_column_count; ++index) {
        const auto sort_mode = _sort_definitions[index].sort_mode;
        const auto column_id = _sort_definitions[index].column;
        const auto column_data_type = input_table->column_data_type(column_id);
        const auto segment = chunk->get_segment(column_id);
        const auto ascending = sort_mode == SortMode::AscendingNullsFirst || sort_mode == SortMode::AscendingNullsLast;
        const auto nulls_first =
            sort_mode == SortMode::AscendingNullsFirst || sort_mode == SortMode::DescendingNullsFirst;
        resolve_data_type(column_data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;
          materialize_segment_as_normalized_keys<ColumnDataType>(*segment, key_offset, ascending, column_stats[index],
                                                                 nulls_first, normalized_key_iter);
        });
        key_offset += column_stats[index].width();
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(materialization_tasks);

  const auto materialization_time = timer.lap();

  const auto comp = [normalized_key_size](const NormalizedKeyRow& lhs, const NormalizedKeyRow& rhs) {
    return lhs.less_than(rhs, normalized_key_size);
  };
  hyrise::ips4o::sort<NormalizedKeyRow>(std::views::all(materialized_rows), comp, _config.max_parallelism,
                                        _config.min_blocks_per_stripe, _config.bucket_count - 1,
                                        _config.samples_per_classifier, _config.block_size);

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
