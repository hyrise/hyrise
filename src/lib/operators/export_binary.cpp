#include "export_binary.hpp"

#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "storage/encoding_type.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_utils.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace {

using namespace opossum;  // NOLINT

// Writes the content of the vector to the ofstream
template <typename T>
void export_values(std::ofstream& ofstream, const pmr_vector<T>& values);

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

template <typename T>
void export_values(std::ofstream& ofstream, const pmr_vector<T>& values) {
  ofstream.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(T));
}

// specialized implementation for string values
template <>
void export_values(std::ofstream& ofstream, const pmr_vector<pmr_string>& values) {
  export_string_values(ofstream, values);
}

// specialized implementation for bool values
template <>
void export_values(std::ofstream& ofstream, const pmr_vector<bool>& values) {
  // Cast to fixed-size format used in binary file
  const auto writable_bools = pmr_vector<BoolAsByteType>(values.begin(), values.end());
  export_values(ofstream, writable_bools);
}

template <typename T>
void export_values(std::ofstream& ofstream, const pmr_concurrent_vector<T>& values) {
  // TODO(all): could be faster if we directly write the values into the stream without prior conversion
  const auto value_block = pmr_vector<T>{values.begin(), values.end()};
  export_values(ofstream, value_block);
}

// Writes a shallow copy of the given value to the ofstream
template <typename T>
void export_value(std::ofstream& ofstream, const T& value) {
  ofstream.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

}  // namespace

namespace opossum {

ExportBinary::ExportBinary(const std::shared_ptr<const AbstractOperator>& in, const std::string& filename)
    : AbstractReadOnlyOperator(OperatorType::ExportBinary, in), _filename(filename) {}

void ExportBinary::write_binary(const Table& table, const std::string& filename) {
  std::ofstream ofstream;
  ofstream.exceptions(std::ofstream::failbit | std::ofstream::badbit);
  ofstream.open(filename, std::ios::binary);

  _write_header(table, ofstream);

  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); chunk_id++) {
    _write_chunk(table, ofstream, chunk_id);
  }
}

const std::string& ExportBinary::name() const {
  static const auto name = std::string{"ExportBinary"};
  return name;
}

std::shared_ptr<const Table> ExportBinary::_on_execute() {
  write_binary(*input_table_left(), _filename);
  return _input_left->get_output();
}

std::shared_ptr<AbstractOperator> ExportBinary::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<ExportBinary>(copied_input_left, _filename);
}

void ExportBinary::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void ExportBinary::_write_header(const Table& table, std::ofstream& ofstream) {
  export_value(ofstream, static_cast<ChunkOffset>(table.max_chunk_size()));
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

void ExportBinary::_write_chunk(const Table& table, std::ofstream& ofstream, const ChunkID& chunk_id) {
  const auto chunk = table.get_chunk(chunk_id);
  Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");
  export_value(ofstream, static_cast<ChunkOffset>(chunk->size()));

  // Iterating over all segments of this chunk and exporting them
  for (ColumnID column_id{0}; column_id < chunk->column_count(); column_id++) {
    resolve_data_and_segment_type(
        *chunk->get_segment(column_id),
        [&](const auto data_type_t, const auto& resolved_segment) { _write_segment(resolved_segment, ofstream); });
  }
}

void ExportBinary::_write_segment(const BaseSegment& base_segment, std::ofstream& ofstream) {
  Fail("Binary export for segment type is not supported yet.");
}

template <typename T>
void ExportBinary::_write_segment(const ValueSegment<T>& value_segment, std::ofstream& ofstream) {
  export_value(ofstream, EncodingType::Unencoded);

  if (value_segment.is_nullable()) {
    export_values(ofstream, value_segment.null_values());
  }

  export_values(ofstream, value_segment.values());
}

void ExportBinary::_write_segment(const ReferenceSegment& reference_segment, std::ofstream& ofstream) {
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
void ExportBinary::_write_segment(const DictionarySegment<T>& dictionary_segment, std::ofstream& ofstream) {
  Assert(dictionary_segment.compressed_vector_type(),
         "Expected DictionarySegment to use vector compression for attribute vector");
  Assert(is_fixed_size_byte_aligned(*dictionary_segment.compressed_vector_type()),
         "Does only support fixed-size byte-aligned compressed attribute vectors.");
  export_value(ofstream, EncodingType::Dictionary);

  // Write attribute vector width
  const auto attribute_vector_width = _compressed_vector_width<T>(dictionary_segment);
  export_value(ofstream, static_cast<AttributeVectorWidth>(attribute_vector_width));

  // Write the dictionary size and dictionary
  export_value(ofstream, static_cast<ValueID::base_type>(dictionary_segment.dictionary()->size()));
  export_values(ofstream, *dictionary_segment.dictionary());

  // Write attribute vector
  Assert(dictionary_segment.compressed_vector_type(),
         "Expected DictionarySegment to use vector compression for attribute vector");
  _export_compressed_vector(ofstream, *dictionary_segment.compressed_vector_type(),
                            *dictionary_segment.attribute_vector());
}

template <typename T>
void ExportBinary::_write_segment(const RunLengthSegment<T>& run_length_segment, std::ofstream& ofstream) {
  export_value(ofstream, EncodingType::RunLength);

  // Write size and values
  export_value(ofstream, static_cast<uint32_t>(run_length_segment.values()->size()));
  export_values(ofstream, *run_length_segment.values());

  // Write NULL values
  export_values(ofstream, *run_length_segment.null_values());

  // Write end positions
  export_values(ofstream, *run_length_segment.end_positions());
}

template <typename T>
void ExportBinary::_write_segment(const FrameOfReferenceSegment<T>& frame_of_reference_segment,
                                  std::ofstream& ofstream) {
  Fail("FrameOfReferenceSegments not implemented for data type");
}

template <>
void ExportBinary::_write_segment(const FrameOfReferenceSegment<int32_t>& frame_of_reference_segment,
                                  std::ofstream& ofstream) {
  export_value(ofstream, EncodingType::FrameOfReference);

  // Write attribute vector width
  const auto offset_value_vector_width = _compressed_vector_width<int32_t>(frame_of_reference_segment);
  export_value(ofstream, static_cast<AttributeVectorWidth>(offset_value_vector_width));

  // Write number of blocks and block minima
  export_value(ofstream, static_cast<uint32_t>(frame_of_reference_segment.block_minima().size()));
  export_values(ofstream, frame_of_reference_segment.block_minima());

  // Write length of the NULL and offset value vectors (i.e., size of segment)
  export_value(ofstream, static_cast<uint32_t>(frame_of_reference_segment.null_values().size()));

  // Write NULL values
  export_values(ofstream, frame_of_reference_segment.null_values());

  // Write offset values
  Assert(frame_of_reference_segment.compressed_vector_type(),
         "Expected FrameOfReference to use vector compression for offset values");
  _export_compressed_vector(ofstream, *frame_of_reference_segment.compressed_vector_type(),
                            frame_of_reference_segment.offset_values());
}

template <typename T>
void ExportBinary::_write_segment(const LZ4Segment<T>& lz4_segment, std::ofstream& ofstream) {
  export_value(ofstream, EncodingType::LZ4);

  // Write num elements (rows in segment)
  export_value(ofstream, static_cast<uint32_t>(lz4_segment.size()));

  // Write number of blocks
  export_value(ofstream, static_cast<uint32_t>(lz4_segment.lz4_blocks().size()));

  if (lz4_segment.lz4_blocks().empty()) {
    // No blocks at all: write just last block size = 0
    export_value(ofstream, uint32_t{0});
  } else {
    // if more than one block, write decompressed block size
    if (lz4_segment.lz4_blocks().size() > 1) {
      export_value(ofstream, static_cast<uint32_t>(lz4_segment.block_size()));
    }
    // Write last decompressed block size
    export_value(ofstream, static_cast<uint32_t>(lz4_segment.last_block_size()));
  }

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

  if (lz4_segment.string_offsets() && *lz4_segment.string_offsets()) {
    // Write string_offset size
    export_value(ofstream, static_cast<uint32_t>((*lz4_segment.string_offsets())->size()));
    // Write string_offset data_size
    export_value(ofstream,
                 static_cast<uint32_t>(
                     dynamic_cast<const SimdBp128Vector&>(*lz4_segment.string_offsets().value()).data().size()));
    // Write string offsets
    _export_compressed_vector(ofstream, *lz4_segment.compressed_vector_type(), *lz4_segment.string_offsets().value());
  } else {
    // Write string_offset size = 0
    export_value(ofstream, uint32_t{0});
  }
}

template <typename T>
uint32_t ExportBinary::_compressed_vector_width(const BaseEncodedSegment& base_encoded_segment) {
  uint32_t vector_width = 0u;
  resolve_encoded_segment_type<T>(base_encoded_segment, [&vector_width](auto& typed_segment) {
    Assert(typed_segment.compressed_vector_type(), "Expected Segment to use vector compression");
    switch (*typed_segment.compressed_vector_type()) {
      case CompressedVectorType::FixedSize4ByteAligned:
        vector_width = 4u;
        break;
      case CompressedVectorType::FixedSize2ByteAligned:
        vector_width = 2u;
        break;
      case CompressedVectorType::FixedSize1ByteAligned:
        vector_width = 1u;
        break;
      default:
        Fail("Export of specified CompressedVectorType is not yet supported");
    }
  });
  return vector_width;
}

void ExportBinary::_export_compressed_vector(std::ofstream& ofstream, const CompressedVectorType type,
                                             const BaseCompressedVector& compressed_vector) {
  switch (type) {
    case CompressedVectorType::FixedSize4ByteAligned:
      export_values(ofstream, dynamic_cast<const FixedSizeByteAlignedVector<uint32_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::FixedSize2ByteAligned:
      export_values(ofstream, dynamic_cast<const FixedSizeByteAlignedVector<uint16_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::FixedSize1ByteAligned:
      export_values(ofstream, dynamic_cast<const FixedSizeByteAlignedVector<uint8_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::SimdBp128:
      export_values(ofstream, dynamic_cast<const SimdBp128Vector&>(compressed_vector).data());
      return;
    default:
      Fail("Any other type should have been caught before.");
  }
}

template <typename T>
size_t ExportBinary::_size(const T& object) {
  return sizeof(object);
}

template <>
size_t ExportBinary::_size(const pmr_string& object) {
  return object.length();
}
}  // namespace opossum
