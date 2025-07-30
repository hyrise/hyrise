#include "key_normalizer.h"

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"

namespace hyrise {
// Portable byte swap implementation for 32-bit integer
inline uint32_t portable_bswap_32(const uint32_t val) {
  return ((val & 0xFF000000) >> 24) | ((val & 0x00FF0000) >> 8) | ((val & 0x0000FF00) << 8) |
         ((val & 0x000000FF) << 24);
}

// Portable byte swap implementation for 64-bit integer
inline uint64_t portable_bswap_64(const uint64_t val) {
  return ((val & 0xFF00000000000000) >> 56) | ((val & 0x00FF000000000000) >> 40) | ((val & 0x0000FF0000000000) >> 24) |
         ((val & 0x000000FF00000000) >> 8) | ((val & 0x00000000FF000000) << 8) | ((val & 0x0000000000FF0000) << 24) |
         ((val & 0x000000000000FF00) << 40) | ((val & 0x00000000000000FF) << 56);
}

template <typename T>
T portable_bswap(T val) {
  if constexpr (sizeof(T) == 4) {
    return portable_bswap_32(val);
  } else if constexpr (sizeof(T) == 8) {
    return portable_bswap_64(val);
  }
  return val;
}

size_t data_type_size(const DataType data_type) {
  switch (data_type) {
    case DataType::Int:
      return sizeof(int32_t);
    case DataType::Long:
      return sizeof(int64_t);
    case DataType::Float:
      return sizeof(float);
    case DataType::Double:
      return sizeof(double);
    case DataType::String:
      return sizeof(pmr_string);
    case DataType::Null:
      return 0;
  }
  Fail("Unsupported data type encountered");
}

KeyNormalizer::KeyNormalizer() {}

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
    const auto chunk_offset = table_offset;

    auto task = std::make_shared<JobTask>([=, &result_buffer, &sort_definitions]() {
      insert_keys_for_chunk(result_buffer, current_chunk, sort_definitions, chunk_offset, chunk_id, tuple_key_size,
                            string_prefix_length);
    });
    tasks.emplace_back(task);
    table_offset += chunk_size;
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  return {std::move(result_buffer), tuple_key_size};
}

// PRIVATE

void KeyNormalizer::insert_keys_for_chunk(std::vector<unsigned char>& buffer, const std::shared_ptr<const Chunk>& chunk,
                                          const std::vector<SortColumnDefinition>& sort_definitions,
                                          const uint64_t row_offset, const ChunkID chunk_id,
                                          const uint32_t tuple_key_size, const uint32_t string_prefix_length) {
  const auto chunk_size = chunk->size();
  std::vector<std::function<void(ChunkOffset)>> column_writers;
  uint32_t component_offset = 0;

  for (const auto& sort_definition : sort_definitions) {
    const auto segment = chunk->get_segment(sort_definition.column);
    const auto data_type = segment->data_type();
    const auto sort_mode = sort_definition.sort_mode;
    const auto descending = sort_mode == SortMode::DescendingNullsFirst || sort_mode == SortMode::DescendingNullsLast;
    const auto nulls_first = sort_mode == SortMode::AscendingNullsFirst || sort_mode == SortMode::DescendingNullsFirst;

    resolve_data_type(data_type, [&](const auto type) {
      using ColumnDataType = typename decltype(type)::type;

      segment_iterate<ColumnDataType>(*segment, [&](const auto& pos) {
        const auto offset = (row_offset + pos.chunk_offset()) * tuple_key_size + component_offset;
        const auto is_null = pos.is_null();


        // Use 0x00 when NullsFirst and 0x01 when NullsLast
        buffer[offset] = static_cast<unsigned char>(is_null != nulls_first);

        if (!is_null) {
          _insert_normalized_value(buffer, pos.value(), offset + 1, descending, string_prefix_length);
        } else {
          std::fill(&buffer[offset + 1], &buffer[offset + tuple_key_size - 1], 0x00);
        }
      });
    });

    component_offset += (data_type == DataType::String ? string_prefix_length : data_type_size(data_type)) + 1;
  }

  // Append RowIDs at the end for tie-breaking
  const auto row_id_offset = tuple_key_size - sizeof(RowID);
  for (auto offset = ChunkOffset{0}; offset < chunk_size; ++offset) {
    const auto buffer_start = (row_offset + offset) * tuple_key_size;
    const RowID row_id{chunk_id, offset};
    std::memcpy(&buffer[buffer_start + row_id_offset], &row_id, sizeof(RowID));
  }
}

template <typename T>
void KeyNormalizer::_insert_normalized_value(std::vector<unsigned char>& buffer, const T value, const uint64_t offset,
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
  // For signed integers, the sign bit must be flipped. This maps the range of signed
  // values (e.g., -128 to 127) to an unsigned range (0 to 255) in a way that
  // preserves their order for a lexicographical byte comparison.
  if constexpr (std::is_signed_v<T>) {
    value ^= (T(1) << (sizeof(T) * 8 - 1));
  }

  // Ensure the byte order is big-endian before writing to the buffer. If not, we swap.
  if constexpr (std::endian::native == std::endian::little) {
    if constexpr (sizeof(T) == 4) {
      value = portable_bswap_32(value);
    }
    if constexpr (sizeof(T) == 8) {
      value = portable_bswap_64(value);
    }
  }

  // For descending order, we simply invert all bits of the value's representation.
  if (descending) {
    value = ~value;
  }
  std::memcpy(buffer.data() + offset, &value, sizeof(T));
}

template <class T>
  requires std::is_floating_point_v<T>
void KeyNormalizer::_insert_floating_point(std::vector<unsigned char>& buffer, T value, uint64_t offset,
                                           const bool descending) {
  using I = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;

  I reinterpreted_val;
  std::memcpy(&reinterpreted_val, &value, sizeof(T));

  // If the float is negative (sign bit is 1), we flip all bits to reverse the sort order.
  // If the float is positive (sign bit is 0), we flip only the sign bit to make it sort after all negatives.
  if (reinterpreted_val & (I(1) << (sizeof(I) * 8 - 1))) {
    reinterpreted_val = ~reinterpreted_val;
  } else {
    reinterpreted_val ^= (I(1) << (sizeof(I) * 8 - 1));
  }

  // Now, call append_integral with the correctly transformed bits. Since `I` is unsigned,
  // the signed-integer logic inside _insert_integral will be skipped.
  _insert_integral(buffer, reinterpreted_val, offset, descending);
}

void KeyNormalizer::_insert_string(std::vector<unsigned char>& buffer, const pmr_string& value, const uint64_t offset,
                                   const bool descending, const uint32_t string_prefix_length) {
  const auto prefix_length = std::min(static_cast<uint32_t>(value.size()), string_prefix_length);
  std::memcpy(&buffer[offset], value.data(), prefix_length);

  // Flip bytes in-place if descending
  if (descending) {
    for (auto i = uint32_t{0}; i < prefix_length; ++i) {
      buffer[offset + i] = static_cast<unsigned char>(~value[offset + i]);
    }
  }

  // Pad the rest with 0x01 or 0xFF depending on sort order
  // todo: choose padding character to distinguish 0x00 from '\0'
  unsigned char pad_char = descending ? 0x00 : 0xFF;
  std::fill(&buffer[offset + prefix_length], &buffer[offset + string_prefix_length], pad_char);
}

template void KeyNormalizer::_insert_normalized_value<int32_t>(std::vector<unsigned char>& buffer, int32_t value,
                                                               uint64_t offset, bool descending,
                                                               uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<int64_t>(std::vector<unsigned char>& buffer, int64_t value,
                                                               uint64_t offset, bool descending,
                                                               uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<float>(std::vector<unsigned char>& buffer, float value,
                                                             uint64_t offset, bool descending,
                                                             uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<double>(std::vector<unsigned char>& buffer, double value,
                                                              uint64_t offset, bool descending,
                                                              uint32_t string_prefix_length);
template void KeyNormalizer::_insert_normalized_value<pmr_string>(std::vector<unsigned char>& buffer, pmr_string value,
                                                                  uint64_t offset, bool descending,
                                                                  uint32_t string_prefix_length);

}  // namespace hyrise
