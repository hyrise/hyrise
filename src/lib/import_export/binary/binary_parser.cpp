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
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"
#include "storage/vector_compression/simd_bp128/oversized_types.hpp"
#include "storage/vector_compression/simd_bp128/simd_bp128_vector.hpp"

#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<Table> BinaryParser::parse(const std::string& filename) {
  std::ifstream file;
  file.open(filename, std::ios::binary);
  file.exceptions(std::ifstream::failbit | std::ifstream::badbit);

  auto [table, chunk_count] = _read_header(file);
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    _import_chunk(file, table);
  }

  return table;
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
  file.read(reinterpret_cast<char*>(readable_bools.data()), readable_bools.size() * sizeof(BoolAsByteType));
  return pmr_vector<bool>(readable_bools.begin(), readable_bools.end());
}

pmr_vector<pmr_string> BinaryParser::_read_string_values(std::ifstream& file, const size_t count) {
  const auto string_lengths = _read_values<size_t>(file, count);
  const auto total_length = std::accumulate(string_lengths.cbegin(), string_lengths.cend(), static_cast<size_t>(0));
  const auto buffer = _read_values<char>(file, total_length);

  pmr_vector<pmr_string> values(count);
  size_t start = 0;

  for (size_t i = 0; i < count; ++i) {
    values[i] = pmr_string(buffer.data() + start, buffer.data() + start + string_lengths[i]);
    start += string_lengths[i];
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

  TableColumnDefinitions output_column_definitions;
  for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
    const auto data_type = data_type_to_string.right.at(std::string{column_data_types[column_id]});
    output_column_definitions.emplace_back(std::string{column_names[column_id]}, data_type,
                                           column_nullables[column_id]);
  }

  auto table = std::make_shared<Table>(output_column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);

  return std::make_pair(table, chunk_count);
}

void BinaryParser::_import_chunk(std::ifstream& file, std::shared_ptr<Table>& table) {
  const auto row_count = _read_value<ChunkOffset>(file);

  Segments output_segments;
  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    output_segments.push_back(
        _import_segment(file, row_count, table->column_data_type(column_id), table->column_is_nullable(column_id)));
  }

  const auto mvcc_data = std::make_shared<MvccData>(row_count, CommitID{0});
  table->append_chunk(output_segments, mvcc_data);
  table->last_chunk()->finalize();
}

std::shared_ptr<BaseSegment> BinaryParser::_import_segment(std::ifstream& file, ChunkOffset row_count,
                                                           DataType data_type, bool is_nullable) {
  std::shared_ptr<BaseSegment> result;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    result = _import_segment<ColumnDataType>(file, row_count, is_nullable);
  });

  return result;
}

template <typename ColumnDataType>
std::shared_ptr<BaseSegment> BinaryParser::_import_segment(std::ifstream& file, ChunkOffset row_count,
                                                           bool is_nullable) {
  const auto column_type = _read_value<EncodingType>(file);

  switch (column_type) {
    case EncodingType::Unencoded:
      return _import_value_segment<ColumnDataType>(file, row_count, is_nullable);
    case EncodingType::Dictionary:
      return _import_dictionary_segment<ColumnDataType>(file, row_count);
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
    default:
      // This case happens if the read column type is not a valid EncodingType.
      Fail("Cannot import column: invalid column type");
  }
}

template <typename T>
std::shared_ptr<ValueSegment<T>> BinaryParser::_import_value_segment(std::ifstream& file, ChunkOffset row_count,
                                                                     bool is_nullable) {
  // TODO(unknown): Ideally _read_values would directly write into a pmr_concurrent_vector so that no conversion is
  // needed
  if (is_nullable) {
    const auto nullables = _read_values<bool>(file, row_count);
    const auto values = _read_values<T>(file, row_count);
    return std::make_shared<ValueSegment<T>>(pmr_concurrent_vector<T>{values.begin(), values.end()},
                                             pmr_concurrent_vector<bool>{nullables.begin(), nullables.end()});
  } else {
    const auto values = _read_values<T>(file, row_count);
    return std::make_shared<ValueSegment<T>>(pmr_concurrent_vector<T>{values.begin(), values.end()});
  }
}

template <typename T>
std::shared_ptr<DictionarySegment<T>> BinaryParser::_import_dictionary_segment(std::ifstream& file,
                                                                               ChunkOffset row_count) {
  const auto attribute_vector_width = _read_value<AttributeVectorWidth>(file);
  const auto dictionary_size = _read_value<ValueID>(file);
  const auto null_value_id = dictionary_size;
  auto dictionary = std::make_shared<pmr_vector<T>>(_read_values<T>(file, dictionary_size));

  auto attribute_vector = _import_attribute_vector(file, row_count, attribute_vector_width);

  return std::make_shared<DictionarySegment<T>>(dictionary, attribute_vector, null_value_id);
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
  const auto attribute_vector_width = _read_value<AttributeVectorWidth>(file);
  const auto block_count = _read_value<uint32_t>(file);
  const auto block_minima = pmr_vector<T>(_read_values<T>(file, block_count));
  const auto size = _read_value<uint32_t>(file);
  const auto null_values = pmr_vector<bool>(_read_values<bool>(file, size));
  auto offset_values = _import_offset_value_vector(file, row_count, attribute_vector_width);

  return std::make_shared<FrameOfReferenceSegment<T>>(block_minima, null_values, std::move(offset_values));
}

template <typename T>
std::shared_ptr<LZ4Segment<T>> BinaryParser::_import_lz4_segment(std::ifstream& file, ChunkOffset row_count) {
  const auto num_elements = _read_value<uint32_t>(file);
  const auto block_count = _read_value<uint32_t>(file);

  uint32_t block_size;
  uint32_t last_block_size;
  if (block_count > 1) {
    block_size = _read_value<uint32_t>(file);
    last_block_size = _read_value<uint32_t>(file);
  } else {
    last_block_size = _read_value<uint32_t>(file);
    block_size = last_block_size;
  }

  const size_t compressed_size = (block_count - 1) * block_size + last_block_size;

  pmr_vector<uint32_t> lz4_block_sizes(_read_values<uint32_t>(file, block_count));

  pmr_vector<pmr_vector<char>> lz4_blocks(block_count);
  for (uint32_t block_index = 0; block_index < block_count; ++block_index) {
    lz4_blocks[block_index] = pmr_vector<char>(_read_values<char>(file, lz4_block_sizes[block_index]));
  }

  const auto null_values_size = _read_value<uint32_t>(file);
  std::optional<pmr_vector<bool>> null_values;
  if (null_values_size != 0) {
    null_values = pmr_vector<bool>(_read_values<bool>(file, null_values_size));
  } else {
    null_values = std::nullopt;
  }

  const auto dictionary_size = _read_value<uint32_t>(file);
  auto dictionary = pmr_vector<char>(_read_values<char>(file, dictionary_size));

  const auto string_offsets_size = _read_value<uint32_t>(file);

  if (string_offsets_size > 0) {
    const auto string_offsets_data_size = _read_value<uint32_t>(file);

    // so far, only SimdBp128 compression is supported
    auto string_offsets =
        std::make_unique<SimdBp128Vector>(_read_values<uint128_t>(file, string_offsets_data_size), string_offsets_size);

    return std::make_shared<LZ4Segment<T>>(std::move(lz4_blocks), std::move(null_values), std::move(dictionary),
                                           std::move(string_offsets), block_size, last_block_size, compressed_size,
                                           num_elements);
  } else {
    if (std::is_same<T, pmr_string>::value) {
      return std::make_shared<LZ4Segment<T>>(std::move(lz4_blocks), std::move(null_values), std::move(dictionary),
                                             nullptr, block_size, last_block_size, compressed_size, num_elements);
    } else {
      return std::make_shared<LZ4Segment<T>>(std::move(lz4_blocks), std::move(null_values), std::move(dictionary),
                                             block_size, last_block_size, compressed_size, num_elements);
    }
  }
}

std::shared_ptr<BaseCompressedVector> BinaryParser::_import_attribute_vector(
    std::ifstream& file, ChunkOffset row_count, AttributeVectorWidth attribute_vector_width) {
  switch (attribute_vector_width) {
    case 1:
      return std::make_shared<FixedSizeByteAlignedVector<uint8_t>>(_read_values<uint8_t>(file, row_count));
    case 2:
      return std::make_shared<FixedSizeByteAlignedVector<uint16_t>>(_read_values<uint16_t>(file, row_count));
    case 4:
      return std::make_shared<FixedSizeByteAlignedVector<uint32_t>>(_read_values<uint32_t>(file, row_count));
    default:
      Fail("Cannot import attribute vector with width: " + std::to_string(attribute_vector_width));
  }
}

std::unique_ptr<const BaseCompressedVector> BinaryParser::_import_offset_value_vector(
    std::ifstream& file, ChunkOffset row_count, AttributeVectorWidth attribute_vector_width) {
  switch (attribute_vector_width) {
    case 1:
      return std::make_unique<FixedSizeByteAlignedVector<uint8_t>>(_read_values<uint8_t>(file, row_count));
    case 2:
      return std::make_unique<FixedSizeByteAlignedVector<uint16_t>>(_read_values<uint16_t>(file, row_count));
    case 4:
      return std::make_unique<FixedSizeByteAlignedVector<uint32_t>>(_read_values<uint32_t>(file, row_count));
    default:
      Fail("Cannot import attribute vector with width: " + std::to_string(attribute_vector_width));
  }
}

}  // namespace opossum
