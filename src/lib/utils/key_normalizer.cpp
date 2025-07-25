#include "key_normalizer.h"


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

KeyNormalizer::KeyNormalizer(std::vector<unsigned char>& buffer) {}

template <typename T>
void KeyNormalizer::insert(std::vector<unsigned char>& buffer, const T value, const uint64_t offset,
                           const NormalizedSortMode sort_mode, const uint32_t string_prefix_length) {
  if constexpr (std::is_integral_v<T>) {
    _insert_integral(buffer, value, offset, sort_mode);
  } else if constexpr (std::is_floating_point_v<T>) {
    _insert_floating_point(buffer, value, offset, sort_mode);
  } else if constexpr (std::is_same_v<std::decay_t<T>, pmr_string>) {
    _insert_string(buffer, value, offset, sort_mode, string_prefix_length);
  }
}

void KeyNormalizer::insert_row_id(std::vector<unsigned char>& buffer, const RowID row_id, const uint64_t offset) {
  std::memcpy(&buffer[offset], &row_id, sizeof(ChunkID) + sizeof(ChunkOffset));
}

void KeyNormalizer::insert_chunk(std::vector<unsigned char>& buffer, const std::shared_ptr<const Chunk>& chunk,
                                 const std::vector<SortColumnDefinition>& sort_definitions,
                                 const uint64_t start_row_index, const ChunkID chunk_id, const uint32_t tuple_key_size,
                                 const uint32_t string_prefix_length, const ChunkOffset chunk_size) {
  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
    // Calculate the starting byte position for the current row's key.
    const auto buffer_row_start = (start_row_index + chunk_offset) * tuple_key_size;
    auto key_offset_in_tuple = uint32_t{0};

    // For each row, build the composite key from the specified sort columns.
    for (const auto& sort_definition : sort_definitions) {
      const auto column_id = sort_definition.column;
      const auto segment = chunk->get_segment(column_id);
      const auto value_variant = (*segment)[chunk_offset];  // Get value at current row.

      const auto normalized_sort_mode = (sort_definition.sort_mode == SortMode::AscendingNullsFirst ||
                                         sort_definition.sort_mode == SortMode::AscendingNullsLast)
                                            ? NormalizedSortMode::Ascending
                                            : NormalizedSortMode::Descending;
      const auto nulls_mode = (sort_definition.sort_mode == SortMode::AscendingNullsFirst ||
                               sort_definition.sort_mode == SortMode::DescendingNullsFirst)
                                  ? NullsMode::NullsFirst
                                  : NullsMode::NullsLast;

      // 1. Insert NULL prefix.
      const bool is_null = variant_is_null(value_variant);
      insert_null_prefix(buffer, is_null, buffer_row_start + key_offset_in_tuple, nulls_mode);
      key_offset_in_tuple += 1;

      // 2. Insert the normalized value.
      const auto column_data_type = segment->data_type();
      auto value_size = 0u;

      // Determine the size of the value to correctly advance the offset.
      if (column_data_type == DataType::String) {
        value_size = string_prefix_length;
      } else {
        resolve_data_type(column_data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;
          value_size = sizeof(ColumnDataType);
        });
      }

      if (!is_null) {
        // Use boost::get to extract the value and insert it.
        resolve_data_type(column_data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;
          insert<ColumnDataType>(buffer, boost::get<ColumnDataType>(value_variant),
                                 buffer_row_start + key_offset_in_tuple, normalized_sort_mode, string_prefix_length);
        });
      } else {
        // For NULLs, pad the key with zeros to maintain a fixed width.
        std::memset(buffer.data() + buffer_row_start + key_offset_in_tuple, 0, value_size);
      }
      key_offset_in_tuple += value_size;
    }

    insert_row_id(buffer, RowID{chunk_id, chunk_offset}, buffer_row_start + key_offset_in_tuple);
  }
}

std::pair<std::vector<unsigned char>, uint64_t> KeyNormalizer::convert_table(
    const std::shared_ptr<const Table>& table, const std::vector<SortColumnDefinition>& sort_definitions,
    const uint32_t string_prefix_length) {
  const auto num_rows = table->row_count();

  // Calculate tuple_key_size based only on the columns we are sorting.
  auto tuple_key_size = uint32_t{0};
  for (const auto& sort_definition : sort_definitions) {
    const auto column_id = sort_definition.column;
    const auto data_type = table->column_data_type(column_id);

    // Add 1 byte for the NULL prefix for each sorted column.
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

  auto result_buffer = std::vector<unsigned char>(tuple_key_size * num_rows);
  const auto chunk_count = table->chunk_count();
  auto table_offset = uint64_t{0};
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto current_chunk = table->get_chunk(chunk_id);
    const auto chunk_size = current_chunk->size();

    // The start offset for this chunk is the number of rows we've already processed.
    insert_chunk(result_buffer, current_chunk, sort_definitions, table_offset, chunk_id, tuple_key_size,
                 string_prefix_length, chunk_size);

    table_offset += chunk_size;
  }

  return {result_buffer, tuple_key_size};
}

RowIdIteratorWithEnd KeyNormalizer::get_iterators(std::vector<unsigned char>& buffer, const uint64_t tuple_key_size) {
  return RowIdIteratorWithEnd{.iterator = RowIdIterator{buffer, tuple_key_size, false},
                              .end = RowIdIterator{buffer, tuple_key_size, true}};
}

// PRIVATE

void KeyNormalizer::insert_null_prefix(std::vector<unsigned char>& buffer, const bool is_null, const uint64_t offset,
                                       const NullsMode nulls_mode) {
  const auto null_prefix = static_cast<unsigned char>(((nulls_mode == NullsMode::NullsFirst) & !is_null) |
                                                      ((nulls_mode == NullsMode::NullsLast) & is_null));

  buffer[offset] = null_prefix;
}

template <class T>
  requires std::is_integral_v<T>
void KeyNormalizer::_insert_integral(std::vector<unsigned char>& buffer, T value, const uint64_t offset,
                                     const NormalizedSortMode sort_mode) {
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
  if (sort_mode == NormalizedSortMode::Descending) {
    value = ~value;
  }
  std::memcpy(buffer.data() + offset, &value, sizeof(T));
}

template <class T>
  requires std::is_floating_point_v<T>
void KeyNormalizer::_insert_floating_point(std::vector<unsigned char>& buffer, T value, uint64_t offset,
                                           NormalizedSortMode sort_mode) {
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
  _insert_integral(buffer, reinterpreted_val, offset, sort_mode);
}

void KeyNormalizer::_insert_string(std::vector<unsigned char>& buffer, const pmr_string value, const uint64_t offset,
                                   const NormalizedSortMode sort_mode, const uint32_t string_prefix_length) {
  const auto len_to_copy = std::min(value.size(), static_cast<u_long>(string_prefix_length));
  std::memcpy(buffer.data() + offset, value.data(), len_to_copy);

  // Pad with 0s if the string is shorter than the prefix to ensure fixed width.
  if (len_to_copy < string_prefix_length) {
    std::memset(buffer.data() + offset + len_to_copy, 0, string_prefix_length - len_to_copy);
  }

  // For descending order, we simply invert all bits of the value's representation.
  if (sort_mode == NormalizedSortMode::Descending) {
    for (auto i = uint32_t{0}; i < string_prefix_length; ++i) {
      buffer[offset + i] = ~buffer[offset + i];
    }
  }
}

template void KeyNormalizer::insert<int32_t>(std::vector<unsigned char>& buffer, int32_t value, uint64_t offset,
                                             NormalizedSortMode sort_mode, uint32_t string_prefix_length);
template void KeyNormalizer::insert<int64_t>(std::vector<unsigned char>& buffer, int64_t value, uint64_t offset,
                                             NormalizedSortMode sort_mode, uint32_t string_prefix_length);
template void KeyNormalizer::insert<float>(std::vector<unsigned char>& buffer, float value, uint64_t offset,
                                           NormalizedSortMode sort_mode, uint32_t string_prefix_length);
template void KeyNormalizer::insert<double>(std::vector<unsigned char>& buffer, double value, uint64_t offset,
                                            NormalizedSortMode sort_mode, uint32_t string_prefix_length);
template void KeyNormalizer::insert<pmr_string>(std::vector<unsigned char>& buffer, pmr_string value, uint64_t offset,
                                                NormalizedSortMode sort_mode, uint32_t string_prefix_length);

}  // namespace hyrise
