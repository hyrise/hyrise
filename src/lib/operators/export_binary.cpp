#include "export_binary.hpp"

#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "import_export/binary.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace {

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
void export_string_values(std::ofstream& ofstream, const std::vector<std::string, Alloc>& values) {
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
void export_values(std::ofstream& ofstream, const opossum::pmr_vector<std::string>& values) {
  export_string_values(ofstream, values);
}
template <>
void export_values(std::ofstream& ofstream, const std::vector<std::string>& values) {
  export_string_values(ofstream, values);
}

// specialized implementation for bool values
template <>
void export_values(std::ofstream& ofstream, const std::vector<bool>& values) {
  // Cast to fixed-size format used in binary file
  const auto writable_bools = std::vector<opossum::BoolAsByteType>(values.begin(), values.end());
  export_values(ofstream, writable_bools);
}

template <typename T>
void export_values(std::ofstream& ofstream, const opossum::pmr_concurrent_vector<T>& values) {
  // TODO(all): could be faster if we directly write the values into the stream without prior conversion
  const auto value_block = std::vector<T>{values.begin(), values.end()};
  ofstream.write(reinterpret_cast<const char*>(value_block.data()), value_block.size() * sizeof(T));
}

// specialized implementation for string values
template <>
void export_values(std::ofstream& ofstream, const opossum::pmr_concurrent_vector<std::string>& values) {
  // TODO(all): could be faster if we directly write the values into the stream without prior conversion
  const auto value_block = std::vector<std::string>{values.begin(), values.end()};
  export_string_values(ofstream, value_block);
}

// specialized implementation for bool values
template <>
void export_values(std::ofstream& ofstream, const opossum::pmr_concurrent_vector<bool>& values) {
  // Cast to fixed-size format used in binary file
  const auto writable_bools = std::vector<opossum::BoolAsByteType>(values.begin(), values.end());
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

const std::string ExportBinary::name() const { return "ExportBinary"; }

std::shared_ptr<const Table> ExportBinary::_on_execute() {
  std::ofstream ofstream;
  ofstream.exceptions(std::ofstream::failbit | std::ofstream::badbit);
  ofstream.open(_filename, std::ios::binary);

  const auto table = _input_left->get_output();
  _write_header(table, ofstream);

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    _write_chunk(table, ofstream, chunk_id);
  }

  return _input_left->get_output();
}

std::shared_ptr<AbstractOperator> ExportBinary::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  return std::make_shared<ExportBinary>(recreated_input_left, _filename);
}

void ExportBinary::_write_header(const std::shared_ptr<const Table>& table, std::ofstream& ofstream) {
  export_value(ofstream, static_cast<ChunkOffset>(table->max_chunk_size()));
  export_value(ofstream, static_cast<ChunkID>(table->chunk_count()));
  export_value(ofstream, static_cast<ColumnID>(table->column_count()));

  std::vector<std::string> column_types(table->column_count());
  std::vector<std::string> column_names(table->column_count());
  std::vector<bool> columns_are_nullable(table->column_count());

  // Transform column types and copy column names in order to write them to the file.
  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    column_types[column_id] = data_type_to_string.left.at(table->column_data_type(column_id));
    column_names[column_id] = table->column_name(column_id);
    columns_are_nullable[column_id] = table->column_is_nullable(column_id);
  }
  export_values(ofstream, column_types);
  export_values(ofstream, columns_are_nullable);
  export_string_values(ofstream, column_names);
}

void ExportBinary::_write_chunk(const std::shared_ptr<const Table>& table, std::ofstream& ofstream,
                                const ChunkID& chunk_id) {
  const auto chunk = table->get_chunk(chunk_id);
  const auto context = std::make_shared<ExportContext>(ofstream);

  export_value(ofstream, static_cast<ChunkOffset>(chunk->size()));

  // Iterating over all columns of this chunk and exporting them
  for (ColumnID column_id{0}; column_id < chunk->column_count(); column_id++) {
    auto visitor =
        make_unique_by_data_type<AbstractColumnVisitor, ExportBinaryVisitor>(table->column_data_type(column_id));
    resolve_data_and_column_type(*chunk->get_column(column_id),
                                 [&](const auto data_type_t, const auto& resolved_column) {
                                   visitor->handle_column(resolved_column, context);
                                 });
  }
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_column(const BaseValueColumn& base_column,
                                                         std::shared_ptr<ColumnVisitorContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);
  const auto& column = static_cast<const ValueColumn<T>&>(base_column);

  export_value(context->ofstream, BinaryColumnType::value_column);

  if (column.is_nullable()) {
    export_values(context->ofstream, column.null_values());
  }

  export_values(context->ofstream, column.values());
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_column(const ReferenceColumn& ref_column,
                                                         std::shared_ptr<ColumnVisitorContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);

  // We materialize reference columns and save them as value columns
  export_value(context->ofstream, BinaryColumnType::value_column);

  // Unfortunately, we have to iterate over all values of the reference column
  // to materialize its contents. Then we can write them to the file
  for (ChunkOffset row = 0; row < ref_column.size(); ++row) {
    export_value(context->ofstream, type_cast<T>(ref_column[row]));
  }
}

// handle_column implementation for string columns
template <>
void ExportBinary::ExportBinaryVisitor<std::string>::handle_column(const ReferenceColumn& ref_column,
                                                                   std::shared_ptr<ColumnVisitorContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);

  // We materialize reference columns and save them as value columns
  export_value(context->ofstream, BinaryColumnType::value_column);

  // If there is no data, we can skip all of the coming steps.
  if (ref_column.size() == 0) return;

  std::stringstream values;
  std::string value;
  std::vector<size_t> string_lengths(ref_column.size());

  // We export the values materialized
  for (ChunkOffset row = 0; row < ref_column.size(); ++row) {
    value = type_cast<std::string>(ref_column[row]);
    string_lengths[row] = value.length();
    values << value;
  }

  export_values(context->ofstream, string_lengths);
  context->ofstream << values.rdbuf();
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_column(const BaseDictionaryColumn& base_column,
                                                         std::shared_ptr<ColumnVisitorContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);

  const auto is_fixed_size_byte_aligned = [&]() {
    switch (base_column.compressed_vector_type()) {
      case CompressedVectorType::FixedSize4ByteAligned:
      case CompressedVectorType::FixedSize2ByteAligned:
      case CompressedVectorType::FixedSize1ByteAligned:
        return true;
      default:
        return false;
    }
  }();

  if (!is_fixed_size_byte_aligned) {
    Fail("Does only support fixed-size byte-aligned compressed attribute vectors.");
  }

  export_value(context->ofstream, BinaryColumnType::dictionary_column);

  const auto attribute_vector_width = [&]() {
    switch (base_column.compressed_vector_type()) {
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

  if (base_column.encoding_type() == EncodingType::FixedStringDictionary) {
    const auto& column = static_cast<const FixedStringDictionaryColumn<std::string>&>(base_column);

    // Write the dictionary size and dictionary
    export_value(context->ofstream, static_cast<ValueID>(column.dictionary()->size()));
    export_values(context->ofstream, *column.dictionary());
  } else {
    const auto& column = static_cast<const DictionaryColumn<T>&>(base_column);

    // Write the dictionary size and dictionary
    export_value(context->ofstream, static_cast<ValueID>(column.dictionary()->size()));
    export_values(context->ofstream, *column.dictionary());
  }

  // Write attribute vector
  _export_attribute_vector(context->ofstream, base_column.compressed_vector_type(), *base_column.attribute_vector());
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_column(const BaseEncodedColumn& base_column,
                                                         std::shared_ptr<ColumnVisitorContext> base_context) {
  Fail("Binary export not implemented yet for encoded columns.");
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
