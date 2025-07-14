#include "key_normalizer.h"

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

KeyNormalizer::KeyNormalizer(std::vector<unsigned char>& buffer) : _buffer(buffer) {}

void KeyNormalizer::append(const std::optional<int32_t>& value, const NormalizedSortMode desc,
                           const NullsMode nulls_first) {
  _append_integral(value, desc, nulls_first);
}

void KeyNormalizer::append(const std::optional<int64_t>& value, const NormalizedSortMode desc,
                           const NullsMode nulls_first) {
  _append_integral(value, desc, nulls_first);
}

void KeyNormalizer::append(const std::optional<float>& value, const NormalizedSortMode desc,
                           const NullsMode nulls_first) {
  _append_floating_point(value, desc, nulls_first);
}

void KeyNormalizer::append(const std::optional<double>& value, const NormalizedSortMode desc,
                           const NullsMode nulls_first) {
  _append_floating_point(value, desc, nulls_first);
}

void KeyNormalizer::append(const std::optional<pmr_string>& value, const NormalizedSortMode desc,
                           const NullsMode nulls_first, const size_t prefix_size) {
  _append_null_prefix(value.has_value(), nulls_first);
  if (!value.has_value()) {
    _buffer.resize(_buffer.size() + prefix_size, 0x00);
    return;
  }

  const auto& str = value.value();
  const auto len_to_copy = std::min(str.length(), prefix_size);

  const auto current_size = _buffer.size();
  _buffer.resize(current_size + prefix_size);

  std::memcpy(_buffer.data() + current_size, str.data(), len_to_copy);

  // Pad with 0s if the string is shorter than the prefix to ensure fixed width.
  if (len_to_copy < prefix_size) {
    std::memset(_buffer.data() + current_size + len_to_copy, 0, prefix_size - len_to_copy);
  }

  // For descending order, we simply invert all bits of the value's representation.
  if (desc == NormalizedSortMode::Descending) {
    for (size_t i = 0; i < prefix_size; ++i) {
      _buffer[current_size + i] = ~_buffer[current_size + i];
    }
  }
}

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

void KeyNormalizer::append_row_id(uint64_t row_id) {
  _append_integral(std::optional(row_id), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
}

void KeyNormalizer::insert_row_id(std::vector<unsigned char>& buffer, const ChunkOffset row_id, const uint64_t offset) {
  _insert_integral(buffer, static_cast<uint64_t>(row_id), offset, NormalizedSortMode::Ascending);
}

void KeyNormalizer::append_chunk(const std::shared_ptr<const Chunk>& chunk,
                                 const std::vector<SortColumnDefinition>& sort_definitions) {
  struct SegmentInformationAndAccessor {
    std::unique_ptr<BaseSegmentAccessor> accessor;
    DataType data_type;
    NormalizedSortMode sort_mode;
    NullsMode nulls_mode;
  };

  const auto num_rows = chunk->size();

  auto segment_information_and_accessors = std::unordered_map<ColumnID, SegmentInformationAndAccessor>{};
  segment_information_and_accessors.reserve(sort_definitions.size());
  for (const auto [column_id, sort_mode] : sort_definitions) {
    const auto segment = chunk->get_segment(column_id);
    const auto data_type = segment->data_type();

    resolve_data_type(data_type, [&](const auto type) {
      using Type = typename decltype(type)::type;

      segment_information_and_accessors.emplace(
          column_id, SegmentInformationAndAccessor{
                         std::move(create_segment_accessor<Type>(segment)), data_type,
                         (sort_mode == SortMode::AscendingNullsFirst | sort_mode == SortMode::AscendingNullsLast)
                             ? NormalizedSortMode::Ascending
                             : NormalizedSortMode::Descending,
                         (sort_mode == SortMode::AscendingNullsFirst | sort_mode == SortMode::DescendingNullsFirst)
                             ? NullsMode::NullsFirst
                             : NullsMode::NullsLast});
    });
  }

  for (auto row_id = ChunkOffset(0); row_id < num_rows; ++row_id) {
    for (const auto [column_id, sort_mode] : sort_definitions) {
      const auto data_type = segment_information_and_accessors[column_id].data_type;

      // There is a method resolve_data_and_segment_type() which will probably be useful here
      resolve_data_type(data_type, [&](const auto type) {
        using Type = typename decltype(type)::type;

        const auto& segment_info = segment_information_and_accessors[column_id];

        const auto& accessor = dynamic_cast<const AbstractSegmentAccessor<Type>&>(*segment_info.accessor);

        append(accessor.access(row_id), segment_info.sort_mode, segment_info.nulls_mode);
      });

      append_row_id(row_id);
    }
  }
}

void KeyNormalizer::insert_chunk(std::vector<unsigned char>& buffer, const std::shared_ptr<const Chunk>& chunk,
                                 const std::vector<SortColumnDefinition>& sort_definitions, const uint64_t offset,
                                 const ChunkOffset last_row_id, const uint32_t tuple_key_size,
                                 const uint32_t string_prefix_length, const ChunkOffset chunk_size) {
  for (const auto sort_definition : sort_definitions) {
    const auto sort_mode = sort_definition.sort_mode;
    const auto normalized_sort_mode =
        (sort_mode == SortMode::AscendingNullsFirst | sort_mode == SortMode::AscendingNullsLast)
            ? NormalizedSortMode::Ascending
            : NormalizedSortMode::Descending;
    const auto nulls_mode = (sort_mode == SortMode::AscendingNullsFirst | sort_mode == SortMode::DescendingNullsFirst)
                                ? NullsMode::NullsFirst
                                : NullsMode::NullsLast;
    auto current_offset = offset;

    segment_iterate(*chunk->get_segment(sort_definition.column), [&](const auto segment_position) {
      const bool is_null = segment_position.is_null();
      insert_null_prefix(buffer, is_null, current_offset, nulls_mode);
      insert(buffer, segment_position.value(), current_offset + 1, normalized_sort_mode, string_prefix_length);
      current_offset += tuple_key_size;
    });
  }

  for (auto i = ChunkOffset{0}; i < chunk_size; ++i) {
    insert_row_id(buffer, ChunkOffset{last_row_id + i}, offset + (i + 1) * tuple_key_size - sizeof(ChunkOffset));
  }
}

void KeyNormalizer::append_table(const std::shared_ptr<const Table>& table,
                                 const std::vector<SortColumnDefinition>& sort_definitions) {
  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto current_chunk = table->get_chunk(chunk_id);

    append_chunk(current_chunk, sort_definitions);
  }
}

std::vector<unsigned char> KeyNormalizer::convert_table(const std::shared_ptr<const Table>& table,
                                                        const std::vector<SortColumnDefinition>& sort_definitions,
                                                        const uint32_t string_prefix_length) {
  const auto num_rows = table->row_count();
  const auto num_columns = table->column_count();
  const auto data_types = table->column_data_types();

  auto tuple_key_size = uint32_t{0};
  for (const auto data_type : data_types) {
    if (data_type == DataType::String) {
      tuple_key_size += string_prefix_length;
    } else {
      resolve_data_type(data_type, [&](const auto type) {
        using Type = typename decltype(type)::type;

        tuple_key_size += sizeof(Type);
      });
    }
  }
  // Add one byte for each column for the NULL prefix and add the size of one ChunkOffset for the row id.
  tuple_key_size += num_columns + sizeof(ChunkOffset);

  auto result_buffer = std::vector<unsigned char>(tuple_key_size * num_rows);

  const auto chunk_count = table->chunk_count();
  auto row_id = ChunkOffset{0};
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto current_chunk = table->get_chunk(chunk_id);
    const auto chunk_size = current_chunk->size();

    insert_chunk(result_buffer, current_chunk, sort_definitions, row_id * tuple_key_size, row_id, tuple_key_size,
                 string_prefix_length, chunk_size);

    row_id += chunk_size;
  }

  return result_buffer;
}

// PRIVATE

void KeyNormalizer::_append_null_prefix(const bool has_value, const NullsMode nulls_first) const {
  unsigned char null_byte;
  if (nulls_first == NullsMode::NullsFirst) {
    null_byte = has_value ? 1 : 0;
  } else {
    null_byte = has_value ? 0 : 1;
  }
  _buffer.push_back(null_byte);
}

void KeyNormalizer::insert_null_prefix(std::vector<unsigned char>& buffer, const bool is_null, const uint64_t offset,
                                       const NullsMode nulls_mode) {
  const auto null_prefix = static_cast<unsigned char>(((nulls_mode == NullsMode::NullsFirst) & !is_null) |
                                                      ((nulls_mode == NullsMode::NullsLast) & is_null));

  buffer[offset] = null_prefix;
}

template <typename T>
void KeyNormalizer::_append_integral(const std::optional<T>& value, NormalizedSortMode desc, NullsMode nulls_first) {
  _append_null_prefix(value.has_value(), nulls_first);
  if (!value.has_value()) {
    // If the value is NULL, we just pad with zeros to maintain a fixed key width.
    _buffer.resize(_buffer.size() + sizeof(T), 0x00);
    return;
  }

  T val = value.value();

  // For signed integers, the sign bit must be flipped. This maps the range of signed
  // values (e.g., -128 to 127) to an unsigned range (0 to 255) in a way that
  // preserves their order for a lexicographical byte comparison.
  if constexpr (std::is_signed_v<T>) {
    val ^= (T(1) << (sizeof(T) * 8 - 1));
  }

  // Ensure the byte order is big-endian before writing to the buffer. If not, we swap.
  if constexpr (std::endian::native == std::endian::little) {
    if constexpr (sizeof(T) == 4) {
      val = portable_bswap_32(val);
    }
    if constexpr (sizeof(T) == 8) {
      val = portable_bswap_64(val);
    }
  }

  const size_t current_size = _buffer.size();
  _buffer.resize(current_size + sizeof(T));
  std::memcpy(_buffer.data() + current_size, &val, sizeof(T));

  // For descending order, we simply invert all bits of the value's representation.
  if (desc == NormalizedSortMode::Descending) {
    for (size_t i = 0; i < sizeof(T); ++i) {
      _buffer[current_size + i] = ~_buffer[current_size + i];
    }
  }
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

template <typename T>
void KeyNormalizer::_append_floating_point(const std::optional<T>& value, NormalizedSortMode desc,
                                           NullsMode nulls_first) {
  static_assert(std::is_floating_point_v<T>, "T must be a floating point type");
  using I = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;

  std::optional<I> int_value;
  if (value.has_value()) {
    I reinterpreted_val;
    std::memcpy(&reinterpreted_val, &(*value), sizeof(T));

    // If the float is negative (sign bit is 1), we flip all bits to reverse the sort order.
    // If the float is positive (sign bit is 0), we flip only the sign bit to make it sort after all negatives.
    if (reinterpreted_val & (I(1) << (sizeof(I) * 8 - 1))) {
      reinterpreted_val = ~reinterpreted_val;
    } else {
      reinterpreted_val ^= (I(1) << (sizeof(I) * 8 - 1));
    }
    int_value = reinterpreted_val;
  }

  // Now, call append_integral with the correctly transformed bits. Since `I` is unsigned,
  // the signed-integer logic inside append_integral will be skipped.
  _append_integral(int_value, desc, nulls_first);
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
  const auto len_to_copy = std::min(value.size(), static_cast<uint64_t>(string_prefix_length));
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
