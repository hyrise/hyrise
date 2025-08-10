#include "key_normalizer.h"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstddef>
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
// Portable byte swap implementation for 32-bit integer
inline uint32_t portable_bswap_32(const uint32_t val) {
  return ((val & 0xFF000000u) >> 24u)
      | ((val & 0x00FF0000u) >> 8u)
      | ((val & 0x0000FF00u) << 8u)
      | ((val & 0x000000FFu) << 24u);
}

// Portable byte swap implementation for 64-bit integer
inline uint64_t portable_bswap_64(const uint64_t val) {
  return ((val & 0xFF00000000000000u) >> 56u)
      | ((val & 0x00FF000000000000u) >> 40u)
      | ((val & 0x0000FF0000000000u) >> 24u)
      | ((val & 0x000000FF00000000u) >> 8u)
      | ((val & 0x00000000FF000000u) << 8u)
      | ((val & 0x0000000000FF0000u) << 24u)
      | ((val & 0x000000000000FF00u) << 40u)
      | ((val & 0x00000000000000FFu) << 56u);
}

template <typename T>
inline T portable_bswap(T val) {
  if constexpr (sizeof(T) == 4) {
    return portable_bswap_32(val);
  } else if constexpr (sizeof(T) == 8) {
    return portable_bswap_64(val);
  }
  return val;
}

inline std::size_t data_type_size(const hyrise::DataType data_type) {
  switch (data_type) {
    case hyrise::DataType::Int:
      return sizeof(int32_t);
    case hyrise::DataType::Long:
      return sizeof(int64_t);
    case hyrise::DataType::Float:
      return sizeof(float);
    case hyrise::DataType::Double:
      return sizeof(double);
    case hyrise::DataType::String:
      return sizeof(hyrise::pmr_string);
    case hyrise::DataType::Null:
      return 0;
  }
  Fail("Unsupported data type encountered");
}
}  // namespace

namespace hyrise {
KeyNormalizer::KeyNormalizer() = default;

std::pair<std::vector<unsigned char>, uint64_t> KeyNormalizer::normalize_keys_for_table(
    const std::shared_ptr<const Table>& table, const std::vector<SortColumnDefinition>& sort_definitions,
    const uint32_t string_prefix_length) {
  // Calculate the key size (in bytes) to create vector with fixed size
  auto tuple_key_size = uint32_t{0};
  for (const auto& sort_definition : sort_definitions) {
    const auto column_id = sort_definition.column;
    const auto data_type = table->column_data_type(column_id);

    // Add 1 byte for NULL marker (used for NULLs first/last)
    tuple_key_size += 1;

    if (data_type == DataType::String) {
      tuple_key_size += string_prefix_length;
    } else {
      resolve_data_type(data_type, [&](const auto type) {
        using Type = typename decltype(type)::type;
        tuple_key_size += sizeof(Type);
      });
    }
  }

  tuple_key_size += sizeof(RowID);

  const auto row_count = table->row_count();
  auto result_buffer = std::vector<unsigned char>(tuple_key_size * row_count);

  std::vector<std::shared_ptr<AbstractTask>> tasks;

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

void KeyNormalizer::_insert_keys_for_chunk(std::vector<unsigned char>& buffer,
                                          const std::shared_ptr<const Chunk>& chunk,
                                          const std::vector<SortColumnDefinition>& sort_definitions,
                                          const uint64_t table_offset, const ChunkID chunk_id,
                                          const uint32_t tuple_key_size, const uint32_t string_prefix_length) {
  const auto chunk_size = chunk->size();
  uint32_t component_offset = 0;

  for (const auto& sort_definition : sort_definitions) {
    const auto segment = chunk->get_segment(sort_definition.column);
    const auto data_type = segment->data_type();
    const auto sort_mode = sort_definition.sort_mode;
    const auto descending =
      sort_mode == SortMode::DescendingNullsFirst || sort_mode == SortMode::DescendingNullsLast;
    const auto nulls_first =
      sort_mode == SortMode::AscendingNullsFirst || sort_mode == SortMode::DescendingNullsFirst;

    const auto component_data_size =
      (data_type == DataType::String ? string_prefix_length : data_type_size(data_type));
    const auto component_total_size = component_data_size + 1;

    resolve_data_type(data_type, [&](const auto type) {
      using ColumnDataType = typename decltype(type)::type;

      segment_iterate<ColumnDataType>(*segment, [&](const auto& pos) {
        const auto offset = ((table_offset + pos.chunk_offset()) * tuple_key_size) + component_offset;
        const auto is_null = pos.is_null();

        // Use 0x00 when NullsFirst and 0x01 when NullsLast
        buffer[offset] = static_cast<unsigned char>(is_null != nulls_first);

        if (!is_null) {
          _insert_normalized_value(buffer, pos.value(), offset + 1, descending, string_prefix_length);
        } else {
          std::fill(&buffer[offset + 1], &buffer[offset + component_total_size], 0x00);
        }
      });
    });

    component_offset += (data_type == DataType::String ? string_prefix_length : data_type_size(data_type)) + 1;
  }

  // Append RowIDs at the end for tie-breaking
  const auto row_id_offset = tuple_key_size - sizeof(RowID);
  for (auto offset = ChunkOffset{0}; offset < chunk_size; ++offset) {
    const auto buffer_start = (table_offset + offset) * tuple_key_size;
    const RowID row_id{chunk_id, offset};
    std::memcpy(&buffer[buffer_start + row_id_offset], &row_id, sizeof(RowID));
  }
}

template <typename T>
void KeyNormalizer::_insert_normalized_value(std::vector<unsigned char>& buffer, const T& value, const uint64_t offset,
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
void KeyNormalizer::_insert_integral(std::vector<unsigned char>& buffer, T value, const uint64_t offset,
                                     const bool descending) {
  using UnsignedType = std::make_unsigned_t<T>;
  auto unsigned_value = std::bit_cast<UnsignedType>(value);

  // For signed integers, the sign bit must be flipped. This maps the range of signed
  // values (e.g., -128 to 127) to an unsigned range (0 to 255) in a way that
  // preserves their order for a lexicographical byte comparison.
  if constexpr (std::is_signed_v<T>) {
    unsigned_value ^= UnsignedType(1) << ((sizeof(T) * 8u) - 1u);
  }

  // Ensure the byte order is big-endian before writing to the buffer. If not, we swap.
  if constexpr (std::endian::native == std::endian::little) {
    unsigned_value = portable_bswap(unsigned_value);
  }

  // For descending order, we simply invert all bits of the value's representation.
  if (descending) {
    unsigned_value = ~unsigned_value;
  }
  std::memcpy(buffer.data() + offset, &unsigned_value, sizeof(UnsignedType));
}

template <class T>
  requires std::is_floating_point_v<T>
void KeyNormalizer::_insert_floating_point(std::vector<unsigned char>& buffer, T value, uint64_t offset,
                                           const bool descending) {
  using UnsignedType = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;

  auto reinterpreted_val = std::bit_cast<UnsignedType>(value);

  // If the float is negative (sign bit is 1), we flip all bits to reverse the sort order.
  // If the float is positive (sign bit is 0), we flip only the sign bit to make it sort after all negatives.
  if (reinterpreted_val & (UnsignedType{1} << ((sizeof(UnsignedType) * 8u) - 1u))) {
    reinterpreted_val = ~reinterpreted_val;
  } else {
    reinterpreted_val ^= (UnsignedType{1} << ((sizeof(UnsignedType) * 8u) - 1u));
  }

  // Now, call append_integral with the correctly transformed bits. Since `UnsignedType` is unsigned,
  // the signed-integer logic inside _insert_integral will be skipped.
  _insert_integral(buffer, reinterpreted_val, offset, descending);
}

void KeyNormalizer::_insert_string(std::vector<unsigned char>& buffer, const pmr_string& value, const uint64_t offset,
                                   const bool descending, const uint32_t string_prefix_length) {
  const auto prefix_length = std::min(static_cast<uint32_t>(value.size()), string_prefix_length);
  std::memcpy(&buffer[offset], value.data(), prefix_length);

  constexpr unsigned char PAD_CHAR = 0x00;
  std::fill(&buffer[offset + prefix_length], &buffer[offset + string_prefix_length], PAD_CHAR);

  if (descending) {
    for (auto i = uint32_t{0}; i < string_prefix_length; ++i) {
      buffer[offset + i] = static_cast<unsigned char>(~buffer[offset + i]);
    }
  }
}

template void KeyNormalizer::_insert_normalized_value<int32_t>(std::vector<unsigned char>& buffer, const int32_t& value,
                                                               uint64_t offset, bool descending,
                                                               uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<int64_t>(std::vector<unsigned char>& buffer, const int64_t& value,
                                                               uint64_t offset, bool descending,
                                                               uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<float>(std::vector<unsigned char>& buffer, const float& value,
                                                             uint64_t offset, bool descending,
                                                             uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<double>(std::vector<unsigned char>& buffer, const double& value,
                                                              uint64_t offset, bool descending,
                                                              uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<pmr_string>(std::vector<unsigned char>& buffer,
                                                                  const pmr_string& value,
                                                                  uint64_t offset, bool descending,
                                                                  uint32_t string_prefix_length);

}  // namespace hyrise
