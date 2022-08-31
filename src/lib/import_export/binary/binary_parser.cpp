#include "binary_parser.hpp"

#include <cstdint>
#include <fstream>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <utility>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/bitpacking/bitpacking_vector.hpp"
#include "storage/vector_compression/fixed_width_integer/fixed_width_integer_vector.hpp"

#include "utils/assert.hpp"

namespace hyrise {

std::shared_ptr<Table> BinaryParser::parse(const std::string& filename) {
  std::ifstream file;
  file.open(filename, std::ios::binary);
  file.exceptions(std::ifstream::failbit | std::ifstream::badbit);

  auto [table, chunk_count] = _read_header(file);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    _import_chunk(file, table);
  }

  return table;
}

template <typename T>
pmr_compact_vector BinaryParser::_read_values_compact_vector(std::ifstream& file, const size_t count) {
  const auto bit_width = _read_value<uint8_t>(file);
  auto values = pmr_compact_vector(bit_width, count);
  file.read(reinterpret_cast<char*>(values.get()), static_cast<int64_t>(values.bytes()));
  return values;
}

template <typename T>
pmr_vector<T> BinaryParser::_read_values(std::ifstream& file, const size_t count) {
  pmr_vector<T> values(count);
  file.read(reinterpret_cast<char*>(values.data()), values.size() * sizeof(T));
  return values;
}

// specialized implementation for string values
template <>
pmr_vector<pmr_string> BinaryParser::_read_values(std::ifstream& file, const size_t count) {
  return _read_string_values(file, count);
}

// specialized implementation for bool values
template <>
pmr_vector<bool> BinaryParser::_read_values(std::ifstream& file, const size_t count) {
  pmr_vector<BoolAsByteType> readable_bools(count);
  file.read(reinterpret_cast<char*>(readable_bools.data()),
            static_cast<int64_t>(readable_bools.size() * sizeof(BoolAsByteType)));
  return {readable_bools.begin(), readable_bools.end()};
}

pmr_vector<pmr_string> BinaryParser::_read_string_values(std::ifstream& file, const size_t count) {
  const auto string_lengths = _read_values<size_t>(file, count);
  const auto total_length = std::accumulate(string_lengths.cbegin(), string_lengths.cend(), static_cast<size_t>(0));
  const auto buffer = _read_values<char>(file, total_length);

  auto values = pmr_vector<pmr_string>{count};
  auto start = size_t{0};
  for (auto index = size_t{0}; index < count; ++index) {
    values[index] = pmr_string{buffer.data() + start, buffer.data() + start + string_lengths[index]};
    start += string_lengths[index];
  }

  return values;
}

template <typename T>
T BinaryParser::_read_value(std::ifstream& file) {
  T result;
  file.read(reinterpret_cast<char*>(&result), sizeof(T));
  return result;
}

std::pair<std::shared_ptr<Table>, ChunkID> BinaryParser::_read_header(std::ifstream& file) {
  const auto chunk_size = _read_value<ChunkOffset>(file);
  const auto chunk_count = _read_value<ChunkID>(file);
  const auto column_count = _read_value<ColumnID>(file);
  const auto column_data_types = _read_values<pmr_string>(file, column_count);
  const auto column_nullables = _read_values<bool>(file, column_count);
  const auto column_names = _read_string_values(file, column_count);

  auto output_column_definitions = TableColumnDefinitions{};
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    const auto data_type = data_type_to_string.right.at(std::string{column_data_types[column_id]});
    output_column_definitions.emplace_back(std::string{column_names[column_id]}, data_type,
                                           column_nullables[column_id]);
  }

  auto table = std::make_shared<Table>(output_column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);

  return std::make_pair(table, chunk_count);
}

void BinaryParser::_import_chunk(std::ifstream& file, std::shared_ptr<Table>& table) {
  const auto row_count = _read_value<ChunkOffset>(file);

  // Import sort column definitions
  const auto num_sorted_columns = _read_value<uint32_t>(file);
  std::vector<SortColumnDefinition> sorted_columns;
  for (ColumnID sorted_column_id{0}; sorted_column_id < num_sorted_columns; ++sorted_column_id) {
    const auto column_id = _read_value<ColumnID>(file);
    const auto sort_mode = _read_value<SortMode>(file);
    sorted_columns.emplace_back(SortColumnDefinition{column_id, sort_mode});
  }

  Segments output_segments;
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    output_segments.push_back(
        _import_segment(file, row_count, table->column_data_type(column_id), table->column_is_nullable(column_id)));
  }

  const auto mvcc_data = std::make_shared<MvccData>(row_count, CommitID{0});
  table->append_chunk(output_segments, mvcc_data);
  table->last_chunk()->finalize();
  if (num_sorted_columns > 0) {
    table->last_chunk()->set_individually_sorted_by(sorted_columns);
  }
}

std::shared_ptr<AbstractSegment> BinaryParser::_import_segment(std::ifstream& file, ChunkOffset row_count,
                                                               DataType data_type, bool column_is_nullable) {
  std::shared_ptr<AbstractSegment> result;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    result = _import_segment<ColumnDataType>(file, row_count, column_is_nullable);
  });

  return result;
}

template <typename ColumnDataType>
std::shared_ptr<AbstractSegment> BinaryParser::_import_segment(std::ifstream& file, ChunkOffset row_count,
                                                               bool column_is_nullable) {
  const auto column_type = _read_value<EncodingType>(file);

  switch (column_type) {
    case EncodingType::Unencoded:
      return _import_value_segment<ColumnDataType>(file, row_count, column_is_nullable);
    case EncodingType::Dictionary:
      return _import_dictionary_segment<ColumnDataType>(file, row_count);
    case EncodingType::FixedStringDictionary:
      if constexpr (encoding_supports_data_type(enum_c<EncodingType, EncodingType::FixedStringDictionary>,
                                                hana::type_c<ColumnDataType>)) {
        return _import_fixed_string_dictionary_segment(file, row_count);
      } else {
        Fail("Unsupported data type for FixedStringDictionary encoding");
      }
    case EncodingType::RunLength:
      return _import_run_length_segment<ColumnDataType>(file, row_count);
    case EncodingType::FrameOfReference:
      if constexpr (encoding_supports_data_type(enum_c<EncodingType, EncodingType::FrameOfReference>,
                                                hana::type_c<ColumnDataType>)) {
        return _import_frame_of_reference_segment<ColumnDataType>(file, row_count);
      } else {
        Fail("Unsupported data type for FOR encoding");
      }
    case EncodingType::LZ4:
      return _import_lz4_segment<ColumnDataType>(file, row_count);
  }

  Fail("Invalid EncodingType");
}

template <typename T>
std::shared_ptr<ValueSegment<T>> BinaryParser::_import_value_segment(std::ifstream& file, ChunkOffset row_count,
                                                                     bool column_is_nullable) {
  if (column_is_nullable) {
    const auto segment_is_nullable = _read_value<bool>(file);
    if (segment_is_nullable) {
      auto nullables = _read_values<bool>(file, row_count);
      auto values = _read_values<T>(file, row_count);
      return std::make_shared<ValueSegment<T>>(std::move(values), std::move(nullables));
    }
  }

  auto values = _read_values<T>(file, row_count);
  return std::make_shared<ValueSegment<T>>(std::move(values));
}

template <typename T>
std::shared_ptr<DictionarySegment<T>> BinaryParser::_import_dictionary_segment(std::ifstream& file,
                                                                               ChunkOffset row_count) {
  const auto compressed_vector_type_id = _read_value<CompressedVectorTypeID>(file);
  const auto dictionary_size = _read_value<ValueID>(file);
  auto dictionary = std::make_shared<pmr_vector<T>>(_read_values<T>(file, dictionary_size));

  auto attribute_vector = _import_attribute_vector(file, row_count, compressed_vector_type_id);

  return std::make_shared<DictionarySegment<T>>(dictionary, attribute_vector);
}

std::shared_ptr<FixedStringDictionarySegment<pmr_string>> BinaryParser::_import_fixed_string_dictionary_segment(
    std::ifstream& file, ChunkOffset row_count) {
  const auto compressed_vector_type_id = _read_value<CompressedVectorTypeID>(file);
  const auto dictionary_size = _read_value<ValueID>(file);
  auto dictionary = _import_fixed_string_vector(file, dictionary_size);
  auto attribute_vector = _import_attribute_vector(file, row_count, compressed_vector_type_id);

  return std::make_shared<FixedStringDictionarySegment<pmr_string>>(dictionary, attribute_vector);
}

template <typename T>
std::shared_ptr<RunLengthSegment<T>> BinaryParser::_import_run_length_segment(std::ifstream& file,
                                                                              ChunkOffset row_count) {
  const auto size = _read_value<uint32_t>(file);
  const auto values = std::make_shared<pmr_vector<T>>(_read_values<T>(file, size));
  const auto null_values = std::make_shared<pmr_vector<bool>>(_read_values<bool>(file, size));
  const auto end_positions = std::make_shared<pmr_vector<ChunkOffset>>(_read_values<ChunkOffset>(file, size));

  return std::make_shared<RunLengthSegment<T>>(values, null_values, end_positions);
}

template <typename T>
std::shared_ptr<FrameOfReferenceSegment<T>> BinaryParser::_import_frame_of_reference_segment(std::ifstream& file,
                                                                                             ChunkOffset row_count) {
  const auto compressed_vector_type_id = _read_value<CompressedVectorTypeID>(file);
  const auto block_count = _read_value<uint32_t>(file);
  const auto block_minima = _read_values<T>(file, block_count);

  const auto null_values_stored = _read_value<BoolAsByteType>(file);
  std::optional<pmr_vector<bool>> null_values;
  if (null_values_stored) {
    null_values = _read_values<bool>(file, row_count);
  }

  auto offset_values = _import_offset_value_vector(file, row_count, compressed_vector_type_id);

  return std::make_shared<FrameOfReferenceSegment<T>>(block_minima, null_values, std::move(offset_values));
}

template <typename T>
std::shared_ptr<LZ4Segment<T>> BinaryParser::_import_lz4_segment(std::ifstream& file, ChunkOffset row_count) {
  const auto num_elements = _read_value<uint32_t>(file);
  const auto block_count = _read_value<uint32_t>(file);
  const auto block_size = _read_value<uint32_t>(file);
  const auto last_block_size = _read_value<uint32_t>(file);

  pmr_vector<uint32_t> lz4_block_sizes(_read_values<uint32_t>(file, block_count));

  const auto compressed_size = std::accumulate(lz4_block_sizes.begin(), lz4_block_sizes.end(), size_t{0});

  pmr_vector<pmr_vector<char>> lz4_blocks(block_count);
  for (uint32_t block_index = 0; block_index < block_count; ++block_index) {
    lz4_blocks[block_index] = _read_values<char>(file, lz4_block_sizes[block_index]);
  }

  const auto null_values_size = _read_value<uint32_t>(file);
  std::optional<pmr_vector<bool>> null_values;
  if (null_values_size != 0) {
    null_values = _read_values<bool>(file, null_values_size);
  } else {
    null_values = std::nullopt;
  }

  const auto dictionary_size = _read_value<uint32_t>(file);
  auto dictionary = _read_values<char>(file, dictionary_size);

  const auto string_offsets_size = _read_value<uint32_t>(file);

  if (string_offsets_size > 0) {
    auto string_offsets = std::make_unique<BitPackingVector>(_read_values_compact_vector<uint32_t>(file, row_count));
    return std::make_shared<LZ4Segment<T>>(std::move(lz4_blocks), std::move(null_values), std::move(dictionary),
                                           std::move(string_offsets), block_size, last_block_size, compressed_size,
                                           num_elements);
  }

  if (std::is_same<T, pmr_string>::value) {
    return std::make_shared<LZ4Segment<T>>(std::move(lz4_blocks), std::move(null_values), std::move(dictionary),
                                           nullptr, block_size, last_block_size, compressed_size, num_elements);
  }

  return std::make_shared<LZ4Segment<T>>(std::move(lz4_blocks), std::move(null_values), std::move(dictionary),
                                         block_size, last_block_size, compressed_size, num_elements);
}

std::shared_ptr<BaseCompressedVector> BinaryParser::_import_attribute_vector(
    std::ifstream& file, const ChunkOffset row_count, const CompressedVectorTypeID compressed_vector_type_id) {
  const auto compressed_vector_type = static_cast<CompressedVectorType>(compressed_vector_type_id);
  switch (compressed_vector_type) {
    case CompressedVectorType::BitPacking:
      return std::make_shared<BitPackingVector>(_read_values_compact_vector<uint32_t>(file, row_count));
    case CompressedVectorType::FixedWidthInteger1Byte:
      return std::make_shared<FixedWidthIntegerVector<uint8_t>>(_read_values<uint8_t>(file, row_count));
    case CompressedVectorType::FixedWidthInteger2Byte:
      return std::make_shared<FixedWidthIntegerVector<uint16_t>>(_read_values<uint16_t>(file, row_count));
    case CompressedVectorType::FixedWidthInteger4Byte:
      return std::make_shared<FixedWidthIntegerVector<uint32_t>>(_read_values<uint32_t>(file, row_count));
    default:
      Fail("Cannot import attribute vector with compressed vector type id: " +
           std::to_string(compressed_vector_type_id));
  }
}

std::unique_ptr<const BaseCompressedVector> BinaryParser::_import_offset_value_vector(
    std::ifstream& file, const ChunkOffset row_count, const CompressedVectorTypeID compressed_vector_type_id) {
  const auto compressed_vector_type = static_cast<CompressedVectorType>(compressed_vector_type_id);
  switch (compressed_vector_type) {
    case CompressedVectorType::BitPacking:
      return std::make_unique<BitPackingVector>(_read_values_compact_vector<uint32_t>(file, row_count));
    case CompressedVectorType::FixedWidthInteger1Byte:
      return std::make_unique<FixedWidthIntegerVector<uint8_t>>(_read_values<uint8_t>(file, row_count));
    case CompressedVectorType::FixedWidthInteger2Byte:
      return std::make_unique<FixedWidthIntegerVector<uint16_t>>(_read_values<uint16_t>(file, row_count));
    case CompressedVectorType::FixedWidthInteger4Byte:
      return std::make_unique<FixedWidthIntegerVector<uint32_t>>(_read_values<uint32_t>(file, row_count));
    default:
      Fail("Cannot import attribute vector with compressed vector type id: " +
           std::to_string(compressed_vector_type_id));
  }
}

std::shared_ptr<FixedStringVector> BinaryParser::_import_fixed_string_vector(std::ifstream& file, const size_t count) {
  const auto string_length = _read_value<uint32_t>(file);
  pmr_vector<char> values(string_length * count);
  file.read(values.data(), static_cast<int64_t>(values.size()));
  return std::make_shared<FixedStringVector>(std::move(values), string_length);
}

}  // namespace hyrise
