#include "export_binary.hpp"

#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "import_export/binary.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
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
template <typename T, typename Alloc>
void export_values(std::ofstream& ofstream, const std::vector<T, Alloc>& values);

/* Writes the given strings to the ofstream. First an array of string lengths is written. After that the string are
 * written without any gaps between them.
 * In order to reduce the number of memory allocations we iterate twice over the string vector.
 * After the first iteration we know the number of byte that must be written to the file and can construct a buffer of
 * this size.
 * This approach is indeed faster than a dynamic approach with a stringstream.
 */
template <typename Alloc>
void export_string_values(std::ofstream& ofstream, const std::vector<pmr_string, Alloc>& values) {
  std::vector<size_t> string_lengths(values.size());
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
  std::vector<char> buffer(total_length);
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

// specialized implementation for string values
template <>
void export_values(std::ofstream& ofstream, const pmr_vector<pmr_string>& values) {
  export_string_values(ofstream, values);
}
template <>
void export_values(std::ofstream& ofstream, const std::vector<pmr_string>& values) {
  export_string_values(ofstream, values);
}

// specialized implementation for bool values
template <>
void export_values(std::ofstream& ofstream, const std::vector<bool>& values) {
  // Cast to fixed-size format used in binary file
  const auto writable_bools = std::vector<BoolAsByteType>(values.begin(), values.end());
  export_values(ofstream, writable_bools);
}

template <typename T>
void export_values(std::ofstream& ofstream, const pmr_concurrent_vector<T>& values) {
  // TODO(all): could be faster if we directly write the values into the stream without prior conversion
  const auto value_block = std::vector<T>{values.begin(), values.end()};
  ofstream.write(reinterpret_cast<const char*>(value_block.data()), value_block.size() * sizeof(T));
}

// specialized implementation for string values
template <>
void export_values(std::ofstream& ofstream, const pmr_concurrent_vector<pmr_string>& values) {
  // TODO(all): could be faster if we directly write the values into the stream without prior conversion
  const auto value_block = std::vector<pmr_string>{values.begin(), values.end()};
  export_string_values(ofstream, value_block);
}

// specialized implementation for bool values
template <>
void export_values(std::ofstream& ofstream, const pmr_concurrent_vector<bool>& values) {
  // Cast to fixed-size format used in binary file
  const auto writable_bools = std::vector<BoolAsByteType>(values.begin(), values.end());
  export_values(ofstream, writable_bools);
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

  std::vector<pmr_string> column_types(table.column_count());
  std::vector<pmr_string> column_names(table.column_count());
  std::vector<bool> columns_are_nullable(table.column_count());

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
  Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

  const auto context = std::make_shared<ExportContext>(ofstream);

  export_value(ofstream, static_cast<ChunkOffset>(chunk->size()));

  // Iterating over all segments of this chunk and exporting them
  for (ColumnID column_id{0}; column_id < chunk->column_count(); column_id++) {
    auto visitor =
        make_unique_by_data_type<AbstractSegmentVisitor, ExportBinaryVisitor>(table.column_data_type(column_id));
    resolve_data_and_segment_type(*chunk->get_segment(column_id),
                                  [&](const auto data_type_t, const auto& resolved_segment) {
                                    visitor->handle_segment(resolved_segment, context);
                                  });
  }
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_segment(const BaseValueSegment& base_segment,
                                                          std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);
  const auto& segment = static_cast<const ValueSegment<T>&>(base_segment);

  export_value(context->ofstream, BinarySegmentType::value_segment);

  if (segment.is_nullable()) {
    export_values(context->ofstream, segment.null_values());
  }

  export_values(context->ofstream, segment.values());
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_segment(const ReferenceSegment& ref_segment,
                                                          std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);

  // We materialize reference segments and save them as value segments
  export_value(context->ofstream, BinarySegmentType::value_segment);

  // Unfortunately, we have to iterate over all values of the reference segment
  // to materialize its contents. Then we can write them to the file
  for (ChunkOffset row = 0; row < ref_segment.size(); ++row) {
    export_value(context->ofstream, boost::get<T>(ref_segment[row]));
  }
}

// handle_segment implementation for string segments
template <>
void ExportBinary::ExportBinaryVisitor<pmr_string>::handle_segment(
    const ReferenceSegment& ref_segment, std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);

  // We materialize reference segments and save them as value segments
  export_value(context->ofstream, BinarySegmentType::value_segment);

  // If there is no data, we can skip all of the coming steps.
  if (ref_segment.size() == 0) return;

  std::stringstream values;
  pmr_string value;
  std::vector<size_t> string_lengths(ref_segment.size());

  // We export the values materialized
  for (ChunkOffset row = 0; row < ref_segment.size(); ++row) {
    value = boost::get<pmr_string>(ref_segment[row]);
    string_lengths[row] = value.length();
    values << value;
  }

  export_values(context->ofstream, string_lengths);
  context->ofstream << values.rdbuf();
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_segment(const BaseDictionarySegment& base_segment,
                                                          std::shared_ptr<SegmentVisitorContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);

  Assert(base_segment.compressed_vector_type(),
         "Expected DictionarySegment to use vector compression for attribute vector");
  Assert(is_fixed_size_byte_aligned(*base_segment.compressed_vector_type()),
         "Does only support fixed-size byte-aligned compressed attribute vectors.");

  export_value(context->ofstream, BinarySegmentType::dictionary_segment);

  const auto attribute_vector_width = [&]() {
    Assert(base_segment.compressed_vector_type(),
           "Expected DictionarySegment to use vector compression for attribute vector");
    switch (*base_segment.compressed_vector_type()) {
      case CompressedVectorType::FixedSize4ByteAligned:
        return 4u;
      case CompressedVectorType::FixedSize2ByteAligned:
        return 2u;
      case CompressedVectorType::FixedSize1ByteAligned:
        return 1u;
      default:
        return 0u;
    }
  }();

  // Write attribute vector width
  export_value(context->ofstream, static_cast<const AttributeVectorWidth>(attribute_vector_width));

  if (base_segment.encoding_type() == EncodingType::FixedStringDictionary) {
    const auto& segment = static_cast<const FixedStringDictionarySegment<pmr_string>&>(base_segment);

    // Write the dictionary size and dictionary
    export_value(context->ofstream, static_cast<ValueID::base_type>(segment.dictionary()->size()));
    export_values(context->ofstream, *segment.dictionary());
  } else {
    const auto& segment = static_cast<const DictionarySegment<T>&>(base_segment);

    // Write the dictionary size and dictionary
    export_value(context->ofstream, static_cast<ValueID::base_type>(segment.dictionary()->size()));
    export_values(context->ofstream, *segment.dictionary());
  }

  // Write attribute vector
  Assert(base_segment.compressed_vector_type(),
         "Expected DictionarySegment to use vector compression for attribute vector");
  _export_attribute_vector(context->ofstream, *base_segment.compressed_vector_type(), *base_segment.attribute_vector());
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_segment(const BaseEncodedSegment& base_segment,
                                                          std::shared_ptr<SegmentVisitorContext> base_context) {
  Fail("Binary export not implemented yet for encoded segments.");
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::_export_attribute_vector(std::ofstream& ofstream,
                                                                    const CompressedVectorType type,
                                                                    const BaseCompressedVector& attribute_vector) {
  switch (type) {
    case CompressedVectorType::FixedSize4ByteAligned:
      export_values(ofstream, dynamic_cast<const FixedSizeByteAlignedVector<uint32_t>&>(attribute_vector).data());
      return;
    case CompressedVectorType::FixedSize2ByteAligned:
      export_values(ofstream, dynamic_cast<const FixedSizeByteAlignedVector<uint16_t>&>(attribute_vector).data());
      return;
    case CompressedVectorType::FixedSize1ByteAligned:
      export_values(ofstream, dynamic_cast<const FixedSizeByteAlignedVector<uint8_t>&>(attribute_vector).data());
      return;
    default:
      Fail("Any other type should have been caught before.");
  }
}

}  // namespace opossum
