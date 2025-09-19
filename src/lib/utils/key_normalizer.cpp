#include "key_normalizer.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "assert.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace {
inline uint32_t byteswap_32(const uint32_t val) {
  return ((val & 0xFF000000u) >> 24u) | ((val & 0x00FF0000u) >> 8u) | ((val & 0x0000FF00u) << 8u) |
         ((val & 0x000000FFu) << 24u);
}

inline uint64_t byteswap_64(const uint64_t val) {
  return ((val & 0xFF00000000000000u) >> 56u) | ((val & 0x00FF000000000000u) >> 40u) |
         ((val & 0x0000FF0000000000u) >> 24u) | ((val & 0x000000FF00000000u) >> 8u) |
         ((val & 0x00000000FF000000u) << 8u) | ((val & 0x0000000000FF0000u) << 24u) |
         ((val & 0x000000000000FF00u) << 40u) | ((val & 0x00000000000000FFu) << 56u);
}

// Swaps the endianness of the given value. From C++23, prefer std::byteswap
// (https://en.cppreference.com/w/cpp/numeric/byteswap.html).
template <typename T>
  requires(sizeof(T) == 4 || sizeof(T) == 8)
inline T byteswap(T val) {
  if constexpr (sizeof(T) == 4) {
    return byteswap_32(val);
  } else if constexpr (sizeof(T) == 8) {
    return byteswap_64(val);
  }
}

constexpr auto PAD_CHAR = std::byte{0x00};
}  // namespace

namespace hyrise {
KeyNormalizer::KeyNormalizer() = default;

std::pair<std::vector<std::byte>, uint64_t> KeyNormalizer::normalize_keys_for_table(
    const std::shared_ptr<const Table>& table, const std::vector<SortColumnDefinition>& sort_definitions,
    const uint32_t string_prefix_length) {
  // Calculate the key size (in bytes) to create vector with fixed size.
  auto tuple_key_size = uint32_t{0};
  for (const auto& sort_definition : sort_definitions) {
    const auto column_id = sort_definition.column;
    const auto data_type = table->column_data_type(column_id);

    // Add 1 byte for NULL marker (used for NULLs first/last).
    tuple_key_size += 1;

    resolve_data_type(data_type, [&](const auto type) {
      using Type = typename decltype(type)::type;
      if constexpr (std::is_same_v<Type, pmr_string>) {
        tuple_key_size += string_prefix_length;
      } else {
        tuple_key_size += sizeof(Type);
      }
    });
  }

  tuple_key_size += sizeof(RowID);

  const auto row_count = table->row_count();
  auto result_buffer = std::vector<std::byte>(tuple_key_size * row_count);

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>();

  const auto chunk_count = table->chunk_count();
  auto table_offset = uint64_t{0};
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto current_chunk = table->get_chunk(chunk_id);
    const auto chunk_size = current_chunk->size();

    auto task = std::make_shared<JobTask>([=, &result_buffer, &sort_definitions]() {
      _insert_keys_for_chunk(result_buffer, current_chunk, sort_definitions, table_offset, chunk_id, tuple_key_size,
                             string_prefix_length);
    });
    tasks.emplace_back(task);
    table_offset += chunk_size;
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  return {std::move(result_buffer), tuple_key_size};
}

// PRIVATE

void KeyNormalizer::_insert_keys_for_chunk(std::vector<std::byte>& buffer, const std::shared_ptr<const Chunk>& chunk,
                                           const std::vector<SortColumnDefinition>& sort_definitions,
                                           const uint64_t table_offset, const ChunkID chunk_id,
                                           const uint32_t tuple_key_size, const uint32_t string_prefix_length) {
  const auto chunk_size = chunk->size();
  auto component_offset = uint32_t{0};

  for (const auto& sort_definition : sort_definitions) {
    const auto segment = chunk->get_segment(sort_definition.column);
    const auto data_type = segment->data_type();
    const auto sort_mode = sort_definition.sort_mode;
    const auto descending = sort_mode == SortMode::DescendingNullsFirst || sort_mode == SortMode::DescendingNullsLast;
    const auto nulls_first = sort_mode == SortMode::AscendingNullsFirst || sort_mode == SortMode::DescendingNullsFirst;

    resolve_data_type(data_type, [&](const auto type) {
      using ColumnDataType = typename decltype(type)::type;

      const auto component_data_size = (data_type == DataType::String ? string_prefix_length : sizeof(ColumnDataType));
      const auto component_total_size = component_data_size + 1;

      segment_iterate<ColumnDataType>(*segment, [&](const auto& pos) {
        const auto buffer_offset_to_beginning_of_tuple =
            ((table_offset + pos.chunk_offset()) * tuple_key_size) + component_offset;
        const auto is_null = pos.is_null();

        // Use 0x00 when NullsFirst and 0x01 when NullsLast.
        buffer[buffer_offset_to_beginning_of_tuple] = static_cast<std::byte>(is_null != nulls_first);

        if (!is_null) {
          _insert_normalized_value(buffer, pos.value(), buffer_offset_to_beginning_of_tuple + 1, descending,
                                   string_prefix_length);
        } else {
          std::fill(&buffer[buffer_offset_to_beginning_of_tuple + 1],
                    &buffer[buffer_offset_to_beginning_of_tuple + component_total_size], PAD_CHAR);
        }
      });

      component_offset += component_total_size;
    });
  }

  // Append RowIDs at the end for tie-breaking.
  const auto row_id_offset = tuple_key_size - sizeof(RowID);
  for (auto current_row = ChunkOffset{0}; current_row < chunk_size; ++current_row) {
    const auto buffer_start = (table_offset + current_row) * tuple_key_size;
    const auto row_id = RowID{chunk_id, current_row};
    std::memcpy(&buffer[buffer_start + row_id_offset], &row_id, sizeof(RowID));
  }
}

template <typename T>
void KeyNormalizer::_insert_normalized_value(std::vector<std::byte>& buffer, const T& value, const uint64_t offset,
                                             const bool descending, const uint32_t string_prefix_length) {
  if constexpr (std::is_integral_v<T>) {
    _insert_integral(buffer, value, offset, descending);
  } else if constexpr (std::is_floating_point_v<T>) {
    _insert_floating_point(buffer, value, offset, descending);
  } else if constexpr (std::is_same_v<std::decay_t<T>, pmr_string>) {
    _insert_string(buffer, value, offset, descending, string_prefix_length);
  }
}

template <class T>
  requires std::is_integral_v<T>
void KeyNormalizer::_insert_integral(std::vector<std::byte>& buffer, T value, const uint64_t offset,
                                     const bool descending) {
  using UnsignedType = std::make_unsigned_t<T>;
  auto unsigned_value = std::bit_cast<UnsignedType>(value);

  // For signed integers, the sign bit must be flipped. This maps the range of signed values (e.g., -128 to 127) to an
  // unsigned range (0 to 255) in a way that preserves their order for a lexicographical byte comparison.
  if constexpr (std::is_signed_v<T>) {
    unsigned_value ^= UnsignedType{1} << ((sizeof(T) * 8u) - 1u);
  }

  // Ensure the byte order is big-endian before writing to the buffer. If not, we swap.
  if constexpr (std::endian::native == std::endian::little) {
    unsigned_value = byteswap(unsigned_value);
  }

  // For descending order, we simply invert all bits of the value's representation.
  if (descending) {
    unsigned_value = ~unsigned_value;
  }
  std::memcpy(&buffer[offset], &unsigned_value, sizeof(UnsignedType));
}

template <class T>
  requires std::is_floating_point_v<T>
void KeyNormalizer::_insert_floating_point(std::vector<std::byte>& buffer, T value, uint64_t offset,
                                           const bool descending) {
  using UnsignedType = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;

  auto reinterpreted_val = std::bit_cast<UnsignedType>(value);

  // If the float is negative (sign bit is 1), we flip all bits to reverse the sort order. If the float is positive
  // (sign bit is 0), we flip only the sign bit to make it sort after all negatives.
  if (reinterpreted_val & (UnsignedType{1} << ((sizeof(UnsignedType) * 8u) - 1u))) {
    reinterpreted_val = ~reinterpreted_val;
  } else {
    reinterpreted_val ^= (UnsignedType{1} << ((sizeof(UnsignedType) * 8u) - 1u));
  }

  // Now, call append_integral with the correctly transformed bits. Since `UnsignedType` is unsigned, the signed-integer
  // logic inside _insert_integral will be skipped.
  _insert_integral(buffer, reinterpreted_val, offset, descending);
}

void KeyNormalizer::_insert_string(std::vector<std::byte>& buffer, const pmr_string& value, const uint64_t offset,
                                   const bool descending, const uint32_t string_prefix_length) {
  const auto prefix_length = std::min(static_cast<uint32_t>(value.size()), string_prefix_length);
  std::memcpy(&buffer[offset], value.data(), prefix_length);

  std::fill(&buffer[offset + prefix_length], &buffer[offset + string_prefix_length], descending ? ~PAD_CHAR : PAD_CHAR);

  if (descending) {
    for (auto i = uint32_t{0}; i < prefix_length; ++i) {
      buffer[offset + i] = ~buffer[offset + i];
    }
  }
}

template void KeyNormalizer::_insert_normalized_value<int32_t>(std::vector<std::byte>& buffer, const int32_t& value,
                                                               uint64_t offset, bool descending,
                                                               uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<int64_t>(std::vector<std::byte>& buffer, const int64_t& value,
                                                               uint64_t offset, bool descending,
                                                               uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<float>(std::vector<std::byte>& buffer, const float& value,
                                                             uint64_t offset, bool descending,
                                                             uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<double>(std::vector<std::byte>& buffer, const double& value,
                                                              uint64_t offset, bool descending,
                                                              uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<pmr_string>(std::vector<std::byte>& buffer,
                                                                  const pmr_string& value, uint64_t offset,
                                                                  bool descending, uint32_t string_prefix_length);

}  // namespace hyrise
