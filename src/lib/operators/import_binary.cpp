#include "import_binary.hpp"

#include <cstdint>
#include <fstream>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <utility>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "import_export/binary.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"
#include "utils/assert.hpp"

namespace opossum {

ImportBinary::ImportBinary(const std::string& filename, const std::optional<std::string>& tablename)
    : AbstractReadOnlyOperator(OperatorType::ImportBinary), _filename(filename), _tablename(tablename) {}

const std::string& ImportBinary::name() const {
  static const auto name = std::string{"ImportBinary"};
  return name;
}

std::shared_ptr<Table> ImportBinary::read_binary(const std::string& filename) {
  std::ifstream file;
  file.open(filename, std::ios::binary);

  Assert(file.is_open(), "ImportBinary: Could not open file " + filename);

  file.exceptions(std::ifstream::failbit | std::ifstream::badbit);

  auto [table, chunk_count] = _read_header(file);
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    _import_chunk(file, table);
  }

  return table;
}

template <typename T>
pmr_vector<T> ImportBinary::_read_values(std::ifstream& file, const size_t count) {
  pmr_vector<T> values(count);
  file.read(reinterpret_cast<char*>(values.data()), values.size() * sizeof(T));
  return values;
}

// specialized implementation for string values
template <>
pmr_vector<pmr_string> ImportBinary::_read_values(std::ifstream& file, const size_t count) {
  return _read_string_values(file, count);
}

// specialized implementation for bool values
template <>
pmr_vector<bool> ImportBinary::_read_values(std::ifstream& file, const size_t count) {
  pmr_vector<BoolAsByteType> readable_bools(count);
  file.read(reinterpret_cast<char*>(readable_bools.data()), readable_bools.size() * sizeof(BoolAsByteType));
  return pmr_vector<bool>(readable_bools.begin(), readable_bools.end());
}

pmr_vector<pmr_string> ImportBinary::_read_string_values(std::ifstream& file, const size_t count) {
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
T ImportBinary::_read_value(std::ifstream& file) {
  T result;
  file.read(reinterpret_cast<char*>(&result), sizeof(T));
  return result;
}

std::shared_ptr<const Table> ImportBinary::_on_execute() {
  if (_tablename && Hyrise::get().storage_manager.has_table(*_tablename)) {
    return Hyrise::get().storage_manager.get_table(*_tablename);
  }

  const auto table = read_binary(_filename);

  if (_tablename) {
    Hyrise::get().storage_manager.add_table(*_tablename, table);
  }

  return table;
}

std::shared_ptr<AbstractOperator> ImportBinary::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<ImportBinary>(_filename, _tablename);
}

void ImportBinary::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::pair<std::shared_ptr<Table>, ChunkID> ImportBinary::_read_header(std::ifstream& file) {
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

void ImportBinary::_import_chunk(std::ifstream& file, std::shared_ptr<Table>& table) {
  const auto row_count = _read_value<ChunkOffset>(file);

  Segments output_segments;
  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    output_segments.push_back(
        _import_segment(file, row_count, table->column_data_type(column_id), table->column_is_nullable(column_id)));
  }

  const auto mvcc_data = std::make_shared<MvccData>(row_count, CommitID{0});
  table->append_chunk(output_segments, mvcc_data);
}

std::shared_ptr<BaseSegment> ImportBinary::_import_segment(std::ifstream& file, ChunkOffset row_count,
                                                           DataType data_type, bool is_nullable) {
  std::shared_ptr<BaseSegment> result;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    result = _import_segment<ColumnDataType>(file, row_count, is_nullable);
  });

  return result;
}

template <typename ColumnDataType>
std::shared_ptr<BaseSegment> ImportBinary::_import_segment(std::ifstream& file, ChunkOffset row_count,
                                                           bool is_nullable) {
  const auto column_type = _read_value<BinarySegmentType>(file);

  switch (column_type) {
    case BinarySegmentType::value_segment:
      return _import_value_segment<ColumnDataType>(file, row_count, is_nullable);
    case BinarySegmentType::dictionary_segment:
      return _import_dictionary_segment<ColumnDataType>(file, row_count);
    default:
      // This case happens if the read column type is not a valid BinarySegmentType.
      Fail("Cannot import column: invalid column type");
  }
}

std::shared_ptr<BaseCompressedVector> ImportBinary::_import_attribute_vector(
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

template <typename T>
std::shared_ptr<ValueSegment<T>> ImportBinary::_import_value_segment(std::ifstream& file, ChunkOffset row_count,
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
std::shared_ptr<DictionarySegment<T>> ImportBinary::_import_dictionary_segment(std::ifstream& file,
                                                                               ChunkOffset row_count) {
  const auto attribute_vector_width = _read_value<AttributeVectorWidth>(file);
  const auto dictionary_size = _read_value<ValueID>(file);
  const auto null_value_id = dictionary_size;
  auto dictionary = std::make_shared<pmr_vector<T>>(_read_values<T>(file, dictionary_size));

  auto attribute_vector = _import_attribute_vector(file, row_count, attribute_vector_width);

  return std::make_shared<DictionarySegment<T>>(dictionary, attribute_vector, null_value_id);
}

}  // namespace opossum
