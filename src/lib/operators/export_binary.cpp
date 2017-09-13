#include "export_binary.hpp"

#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "import_export/binary.hpp"
#include "storage/reference_column.hpp"

#include "resolve_type.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace {

// Writes the content of the vector to the ofstream
template <typename T, typename Alloc>
void _export_values(std::ofstream& ofstream, const std::vector<T, Alloc>& values);

/* Writes the given strings to the ofstream. First an array of string lengths is written. After that the string are
 * written without any gaps between them.
 * In order to reduce the number of memory allocations we iterate twice over the string vector.
 * After the first iteration we know the number of byte that must be written to the file and can construct a buffer of
 * this size.
 * This approach is indeed faster than a dynamic approach with a stringstream.
 */
template <typename T = opossum::StringLength, typename Alloc>
void _export_string_values(std::ofstream& ofstream, const std::vector<std::string, Alloc>& values) {
  std::vector<T> string_lengths(values.size());
  size_t total_length = 0;

  // Save the length of each string.
  for (size_t i = 0; i < values.size(); ++i) {
    string_lengths[i] = values[i].size();
    total_length += values[i].size();
  }

  _export_values(ofstream, string_lengths);

  // We do not have to iterate over values if all strings are empty.
  if (total_length == 0) return;

  // Write all string contents into to buffer.
  std::vector<char> buffer(total_length);
  size_t start = 0;
  for (const auto& str : values) {
    std::memcpy(buffer.data() + start, str.data(), str.size());
    start += str.size();
  }

  _export_values(ofstream, buffer);
}

template <typename T, typename Alloc>
void _export_values(std::ofstream& ofstream, const std::vector<T, Alloc>& values) {
  ofstream.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(T));
}

// specialized implementation for string values
template <>
void _export_values(std::ofstream& ofstream, const opossum::pmr_vector<std::string>& values) {
  _export_string_values(ofstream, values);
}
template <>
void _export_values(std::ofstream& ofstream, const std::vector<std::string>& values) {
  _export_string_values(ofstream, values);
}

template <typename T, typename Alloc>
void _export_values(std::ofstream& ofstream, const tbb::concurrent_vector<T, Alloc>& values) {
  // TODO(all): could be faster if we directly write the values into the stream without prior conversion
  const auto value_block = std::vector<T>{values.begin(), values.end()};
  ofstream.write(reinterpret_cast<const char*>(value_block.data()), value_block.size() * sizeof(T));
}

// specialized implementation for string values
template <>
void _export_values(std::ofstream& ofstream, const opossum::pmr_concurrent_vector<std::string>& values) {
  // TODO(all): could be faster if we directly write the values into the stream without prior conversion
  const auto value_block = std::vector<std::string>{values.begin(), values.end()};
  _export_string_values(ofstream, value_block);
}

// Writes a shallow copy of the given value to the ofstream
template <typename T>
void _export_value(std::ofstream& ofstream, const T& value) {
  ofstream.write(reinterpret_cast<const char*>(&value), sizeof(T));
}
}  // namespace

namespace opossum {

ExportBinary::ExportBinary(const std::shared_ptr<const AbstractOperator> in, const std::string& filename)
    : AbstractReadOnlyOperator(in), _filename(filename) {}

const std::string ExportBinary::name() const { return "ExportBinary"; }

uint8_t ExportBinary::num_in_tables() const { return 1; }

uint8_t ExportBinary::num_out_tables() const { return 1; }

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

void ExportBinary::_write_header(const std::shared_ptr<const Table>& table, std::ofstream& ofstream) {
  _export_value(ofstream, static_cast<ChunkOffset>(table->chunk_size()));
  _export_value(ofstream, static_cast<ChunkID>(table->chunk_count()));
  _export_value(ofstream, static_cast<ColumnID>(table->col_count()));

  std::vector<std::string> column_types(table->col_count());
  std::vector<std::string> column_names(table->col_count());

  // Transform column types and copy column names in order to write them to the file.
  for (ColumnID column_id{0}; column_id < table->col_count(); ++column_id) {
    column_types[column_id] = table->column_type(column_id);
    column_names[column_id] = table->column_name(column_id);
  }
  _export_values(ofstream, column_types);
  _export_string_values<ColumnNameLength>(ofstream, column_names);
}

void ExportBinary::_write_chunk(const std::shared_ptr<const Table>& table, std::ofstream& ofstream,
                                const ChunkID& chunkId) {
  const auto& chunk = table->get_chunk(chunkId);
  const auto context = std::make_shared<ExportContext>(ofstream);

  _export_value(ofstream, static_cast<ChunkOffset>(chunk.size()));

  // Iterating over all columns of this chunk and exporting them
  for (ColumnID col_id{0}; col_id < chunk.col_count(); col_id++) {
    auto visitor = make_unique_by_column_type<ColumnVisitable, ExportBinaryVisitor>(table->column_type(col_id));
    chunk.get_column(col_id)->visit(*visitor, context);
  }
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_value_column(BaseColumn& base_column,
                                                               std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);
  const auto& column = static_cast<ValueColumn<T>&>(base_column);

  _export_value(context->ofstream, BinaryColumnType::value_column);
  _export_values(context->ofstream, column.values());
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_reference_column(
    ReferenceColumn& ref_column, std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);

  // We materialize reference columns and save them as value columns
  _export_value(context->ofstream, BinaryColumnType::value_column);

  // Unfortunately, we have to iterate over all values of the reference column
  // to materialize its contents. Then we can write them to the file
  for (ChunkOffset row = 0; row < ref_column.size(); ++row) {
    _export_value(context->ofstream, type_cast<T>(ref_column[row]));
  }
}

// handle_reference_column implementation for string columns
template <>
void ExportBinary::ExportBinaryVisitor<std::string>::handle_reference_column(
    ReferenceColumn& ref_column, std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);

  // We materialize reference columns and save them as value columns
  _export_value(context->ofstream, BinaryColumnType::value_column);

  // If there is no data, we can skip all of the coming steps.
  if (ref_column.size() == 0) return;

  std::stringstream values;
  std::string value;
  std::vector<StringLength> string_lengths(ref_column.size());

  // We export the values materialized
  for (ChunkOffset row = 0; row < ref_column.size(); ++row) {
    value = type_cast<std::string>(ref_column[row]);
    string_lengths[row] = static_cast<StringLength>(value.length());
    values << value;
  }

  _export_values(context->ofstream, string_lengths);
  context->ofstream << values.rdbuf();
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::handle_dictionary_column(
    BaseColumn& base_column, std::shared_ptr<ColumnVisitableContext> base_context) {
  auto context = std::static_pointer_cast<ExportContext>(base_context);
  const auto& column = static_cast<DictionaryColumn<T>&>(base_column);

  _export_value(context->ofstream, BinaryColumnType::dictionary_column);
  _export_value(context->ofstream, static_cast<const AttributeVectorWidth>(column.attribute_vector()->width()));

  // Write the dictionary size and dictionary
  _export_value(context->ofstream, static_cast<ValueID>(column.unique_values_count()));
  _export_values(context->ofstream, *column.dictionary());

  _export_attribute_vector(context->ofstream, *column.attribute_vector());
}

template <typename T>
void ExportBinary::ExportBinaryVisitor<T>::_export_attribute_vector(std::ofstream& ofstream,
                                                                    const BaseAttributeVector& attribute_vector) {
  switch (attribute_vector.width()) {
    case 1:
      _export_values(ofstream, dynamic_cast<const FittedAttributeVector<uint8_t>&>(attribute_vector).attributes());
      break;
    case 2:
      _export_values(ofstream, dynamic_cast<const FittedAttributeVector<uint16_t>&>(attribute_vector).attributes());
      break;
    case 4:
    default:
      _export_values(ofstream, dynamic_cast<const FittedAttributeVector<uint32_t>&>(attribute_vector).attributes());
      break;
  }
}

}  // namespace opossum
