#include "binary_writer.hpp"

#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "storage/encoding_type.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/vector_compression/bitpacking/bitpacking_vector.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "storage/vector_compression/fixed_width_integer/fixed_width_integer_utils.hpp"
#include "storage/vector_compression/fixed_width_integer/fixed_width_integer_vector.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace {

using namespace opossum;  // NOLINT

// Writes the content of the vector to the ofstream
template <typename T, typename Alloc>
void export_values(std::ofstream& ofstream, const std::vector<T, Alloc>& values);

/* Writes the given strings to the ofstream. First an array of string lengths is written. After that the strings are
 * written without any gaps between them.
 * In order to reduce the number of memory allocations we iterate twice over the string vector.
 * After the first iteration we know the number of byte that must be written to the file and can construct a buffer of
 * this size.
 * This approach is indeed faster than a dynamic approach with a stringstream.
 */
void export_string_values(std::ofstream& ofstream, const pmr_vector<pmr_string>& values) {
  pmr_vector<size_t> string_lengths(values.size());
  size_t total_length = 0;

  // Save the length of each string.
  for (size_t i = 0; i < values.size(); ++i) {
    string_lengths[i] = values[i].size();
    total_length += values[i].size();
  }

  export_values(ofstream, string_lengths);

  // We do not have to iterate over values if all strings are empty.
  if (total_length == 0) return;

  // Write all string contents into to buffer.
  pmr_vector<char> buffer(total_length);
  size_t start = 0;
  for (const auto& str : values) {
    std::memcpy(buffer.data() + start, str.data(), str.size());
    start += str.size();
  }

  export_values(ofstream, buffer);
}

template <typename T, typename Alloc>
void export_values(std::ofstream& ofstream, const std::vector<T, Alloc>& values) {
  ofstream.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(T));
}

void export_values(std::ofstream& ofstream, const FixedStringVector& values) {
  ofstream.write(values.data(), values.size() * values.string_length());
}

// specialized implementation for string values
template <>
void export_values(std::ofstream& ofstream, const pmr_vector<pmr_string>& values) {
  export_string_values(ofstream, values);
}

// specialized implementation for bool values
template <typename Alloc>
void export_values(std::ofstream& ofstream, const std::vector<bool, Alloc>& values) {
  // Cast to fixed-size format used in binary file
  const auto writable_bools = pmr_vector<BoolAsByteType>(values.begin(), values.end());
  export_values(ofstream, writable_bools);
}

// Writes a shallow copy of the given value to the ofstream
template <typename T>
void export_value(std::ofstream& ofstream, const T& value) {
  ofstream.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

void export_compact_vector(std::ofstream& ofstream, const pmr_compact_vector& values) {
  export_value(ofstream, static_cast<uint8_t>(values.bits()));
  ofstream.write(reinterpret_cast<const char*>(values.get()), values.bytes());
}

}  // namespace

namespace opossum {

void BinaryWriter::write(const Table& table, const std::string& filename) {
  std::ofstream ofstream;
  ofstream.exceptions(std::ofstream::failbit | std::ofstream::badbit);
  ofstream.open(filename, std::ios::binary);

  _write_header(table, ofstream);

  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); chunk_id++) {
    _write_chunk(table, ofstream, chunk_id);
  }
}

void BinaryWriter::_write_header(const Table& table, std::ofstream& ofstream) {
  const auto target_chunk_size = table.type() == TableType::Data ? table.target_chunk_size() : Chunk::DEFAULT_SIZE;
  export_value(ofstream, static_cast<ChunkOffset>(target_chunk_size));
  export_value(ofstream, static_cast<ChunkID::base_type>(table.chunk_count()));
  export_value(ofstream, static_cast<ColumnID::base_type>(table.column_count()));

  pmr_vector<pmr_string> column_types(table.column_count());
  pmr_vector<pmr_string> column_names(table.column_count());
  pmr_vector<bool> columns_are_nullable(table.column_count());

  // Transform column types and copy column names in order to write them to the file.
  for (ColumnID column_id{0}; column_id < table.column_count(); ++column_id) {
    column_types[column_id] = data_type_to_string.left.at(table.column_data_type(column_id));
    column_names[column_id] = table.column_name(column_id);
    columns_are_nullable[column_id] = table.column_is_nullable(column_id);
  }
  export_values(ofstream, column_types);
  export_values(ofstream, columns_are_nullable);
  export_string_values(ofstream, column_names);
}

void BinaryWriter::_write_chunk(const Table& table, std::ofstream& ofstream, const ChunkID& chunk_id) {
  const auto chunk = table.get_chunk(chunk_id);
  Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");
  export_value(ofstream, static_cast<ChunkOffset>(chunk->size()));

  // Export sort column definitions
  const auto& sorted_columns = chunk->individually_sorted_by();
  export_value(ofstream, static_cast<uint32_t>(sorted_columns.size()));
  for (const auto& [column, sort_mode] : sorted_columns) {
    export_value(ofstream, column);
    export_value(ofstream, sort_mode);
  }

  // Iterating over all segments of this chunk and exporting them
  for (ColumnID column_id{0}; column_id < chunk->column_count(); column_id++) {
    resolve_data_and_segment_type(*chunk->get_segment(column_id),
                                  [&](const auto data_type_t, const auto& resolved_segment) {
                                    _write_segment(resolved_segment, table.column_is_nullable(column_id), ofstream);
                                  });
  }
}

template <typename T>
void BinaryWriter::_write_segment(const ValueSegment<T>& value_segment, bool column_is_nullable,
                                  std::ofstream& ofstream) {
  export_value(ofstream, EncodingType::Unencoded);

  if (column_is_nullable) {
    export_value(ofstream, value_segment.is_nullable());
  }

  if (value_segment.is_nullable()) {
    export_values(ofstream, value_segment.null_values());
  }

  export_values(ofstream, value_segment.values());
}

void BinaryWriter::_write_segment(const ReferenceSegment& reference_segment, bool column_is_nullable,
                                  std::ofstream& ofstream) {
  // We materialize reference segments and save them as value segments
  export_value(ofstream, EncodingType::Unencoded);

  if (reference_segment.size() == 0) return;
  resolve_data_type(reference_segment.data_type(), [&](auto type) {
    using SegmentDataType = typename decltype(type)::type;
    auto iterable = ReferenceSegmentIterable<SegmentDataType, EraseReferencedSegmentType::No>{reference_segment};

    if (reference_segment.data_type() == DataType::String) {
      std::stringstream values;
      pmr_vector<size_t> string_lengths(reference_segment.size());

      // We export the values materialized
      iterable.for_each([&](const auto& value) {
        string_lengths.push_back(_size(value.value()));
        values << value.value();
      });

      export_values(ofstream, string_lengths);
      ofstream << values.rdbuf();

    } else {
      // Unfortunately, we have to iterate over all values of the reference segment
      // to materialize its contents. Then we can write them to the file
      iterable.for_each([&](const auto& value) { export_value(ofstream, value.value()); });
    }
  });
}

template <typename T>
void BinaryWriter::_write_segment(const DictionarySegment<T>& dictionary_segment, bool column_is_nullable,
                                  std::ofstream& ofstream) {
  export_value(ofstream, EncodingType::Dictionary);

  // Write attribute vector compression id
  const auto compressed_vector_type_id = _compressed_vector_type_id<T>(dictionary_segment);
  export_value(ofstream, compressed_vector_type_id);

  // Write the dictionary size and dictionary
  export_value(ofstream, static_cast<ValueID::base_type>(dictionary_segment.dictionary()->size()));
  export_values(ofstream, *dictionary_segment.dictionary());

  // Write attribute vector
  _export_compressed_vector(ofstream, *dictionary_segment.compressed_vector_type(),
                            *dictionary_segment.attribute_vector());
}

template <typename T>
void BinaryWriter::_write_segment(const FixedStringDictionarySegment<T>& fixed_string_dictionary_segment,
                                  bool column_is_nullable, std::ofstream& ofstream) {
  export_value(ofstream, EncodingType::FixedStringDictionary);

  // Write attribute vector compression id
  const auto compressed_vector_type_id = _compressed_vector_type_id<T>(fixed_string_dictionary_segment);
  export_value(ofstream, compressed_vector_type_id);

  // Write the dictionary size, string length and dictionary
  const auto dictionary_size = fixed_string_dictionary_segment.fixed_string_dictionary()->size();
  const auto string_length = fixed_string_dictionary_segment.fixed_string_dictionary()->string_length();
  export_value(ofstream, static_cast<ValueID::base_type>(dictionary_size));
  export_value(ofstream, static_cast<uint32_t>(string_length));
  export_values(ofstream, *fixed_string_dictionary_segment.fixed_string_dictionary());

  // Write attribute vector
  _export_compressed_vector(ofstream, *fixed_string_dictionary_segment.compressed_vector_type(),
                            *fixed_string_dictionary_segment.attribute_vector());
}

template <typename T>
void BinaryWriter::_write_segment(const RunLengthSegment<T>& run_length_segment, bool column_is_nullable,
                                  std::ofstream& ofstream) {
  export_value(ofstream, EncodingType::RunLength);

  // Write size and values
  export_value(ofstream, static_cast<uint32_t>(run_length_segment.values()->size()));
  export_values(ofstream, *run_length_segment.values());

  // Write NULL values
  export_values(ofstream, *run_length_segment.null_values());

  // Write end positions
  export_values(ofstream, *run_length_segment.end_positions());
}

template <>
void BinaryWriter::_write_segment(const FrameOfReferenceSegment<int32_t>& frame_of_reference_segment,
                                  bool column_is_nullable, std::ofstream& ofstream) {
  export_value(ofstream, EncodingType::FrameOfReference);

  // Write attribute vector compression id
  const auto compressed_vector_type_id = _compressed_vector_type_id<int32_t>(frame_of_reference_segment);
  export_value(ofstream, compressed_vector_type_id);

  // Write number of blocks and block minima
  export_value(ofstream, static_cast<uint32_t>(frame_of_reference_segment.block_minima().size()));
  export_values(ofstream, frame_of_reference_segment.block_minima());

  // Write flag if optional NULL value vector is written
  export_value(ofstream, static_cast<BoolAsByteType>(frame_of_reference_segment.null_values().has_value()));
  if (frame_of_reference_segment.null_values()) {
    // Write NULL values
    export_values(ofstream, *frame_of_reference_segment.null_values());
  }

  // Write offset values
  _export_compressed_vector(ofstream, *frame_of_reference_segment.compressed_vector_type(),
                            frame_of_reference_segment.offset_values());
}

template <typename T>
void BinaryWriter::_write_segment(const LZ4Segment<T>& lz4_segment, bool column_is_nullable, std::ofstream& ofstream) {
  export_value(ofstream, EncodingType::LZ4);

  // Write num elements (rows in segment)
  export_value(ofstream, static_cast<uint32_t>(lz4_segment.size()));

  // Write number of blocks
  export_value(ofstream, static_cast<uint32_t>(lz4_segment.lz4_blocks().size()));

  // Write block size
  export_value(ofstream, static_cast<uint32_t>(lz4_segment.block_size()));

  // Write last block size
  export_value(ofstream, static_cast<uint32_t>(lz4_segment.last_block_size()));

  // Write compressed size for each LZ4 Block
  for (const auto& lz4_block : lz4_segment.lz4_blocks()) {
    export_value(ofstream, static_cast<uint32_t>(lz4_block.size()));
  }

  // Write LZ4 Blocks
  for (const auto& lz4_block : lz4_segment.lz4_blocks()) {
    export_values(ofstream, lz4_block);
  }

  if (lz4_segment.null_values()) {
    // Write NULL value size
    export_value(ofstream, static_cast<uint32_t>(lz4_segment.null_values()->size()));
    // Write NULL values
    export_values(ofstream, *lz4_segment.null_values());
  } else {
    // No NULL values
    export_value(ofstream, uint32_t{0});
  }

  // Write dictionary size
  export_value(ofstream, static_cast<uint32_t>(lz4_segment.dictionary().size()));

  // Write dictionary
  export_values(ofstream, lz4_segment.dictionary());

  if (lz4_segment.string_offsets()) {
    // Write string_offset size
    export_value(ofstream, static_cast<uint32_t>(lz4_segment.string_offsets()->size()));
    // Write string_offset data_size
    export_compact_vector(ofstream, dynamic_cast<const BitPackingVector&>(*lz4_segment.string_offsets()).data());
  } else {
    // Write string_offset size = 0
    export_value(ofstream, uint32_t{0});
  }
}

template <typename T>
CompressedVectorTypeID BinaryWriter::_compressed_vector_type_id(
    const AbstractEncodedSegment& abstract_encoded_segment) {
  uint8_t compressed_vector_type_id = 0u;
  resolve_encoded_segment_type<T>(abstract_encoded_segment, [&compressed_vector_type_id](auto& typed_segment) {
    const auto compressed_vector_type = typed_segment.compressed_vector_type();
    Assert(compressed_vector_type, "Expected Segment to use vector compression");
    switch (*compressed_vector_type) {
      case CompressedVectorType::FixedWidthInteger4Byte:
      case CompressedVectorType::FixedWidthInteger2Byte:
      case CompressedVectorType::FixedWidthInteger1Byte:
      case CompressedVectorType::BitPacking:
        compressed_vector_type_id = static_cast<uint8_t>(*compressed_vector_type);
        break;
      default:
        Fail("Export of specified CompressedVectorType is not yet supported");
    }
  });
  return compressed_vector_type_id;
}

void BinaryWriter::_export_compressed_vector(std::ofstream& ofstream, const CompressedVectorType type,
                                             const BaseCompressedVector& compressed_vector) {
  switch (type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
      export_values(ofstream, dynamic_cast<const FixedWidthIntegerVector<uint32_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::FixedWidthInteger2Byte:
      export_values(ofstream, dynamic_cast<const FixedWidthIntegerVector<uint16_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::FixedWidthInteger1Byte:
      export_values(ofstream, dynamic_cast<const FixedWidthIntegerVector<uint8_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::BitPacking:
      export_compact_vector(ofstream, dynamic_cast<const BitPackingVector&>(compressed_vector).data());
      return;
    default:
      Fail("Any other type should have been caught before.");
  }
}

template <typename T>
size_t BinaryWriter::_size(const T& object) {
  return sizeof(object);
}

template <>
size_t BinaryWriter::_size(const pmr_string& object) {
  return object.length();
}

}  // namespace opossum
