#pragma once

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "storage/base_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

/*
 * This operator reads a Opossum binary file and creates a table from that input.
 * If parameter tablename provided, the imported table is stored in the StorageManager. If a table with this name
 * already exists, it is returned and no import is performed.
 *
 * Note: ImportBinary does not support null values at the moment
 */
class ImportBinary : public AbstractReadOnlyOperator {
 public:
  explicit ImportBinary(const std::string& filename, const std::optional<std::string>& tablename = std::nullopt);

  static std::shared_ptr<Table> read_binary(const std::string& filename);

  /*
   * Reads the given binary file. The file must be in the following form:
   *
   * --------------
   * |   Header   |
   * |------------|
   * |   Chunks¹  |
   * --------------
   *
   * ¹ Zero or more chunks
   */
  std::shared_ptr<const Table> _on_execute() final;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  // Returns the name of the operator
  const std::string& name() const final;

 private:
  /*
   * Reads the header from the given file.
   * Creates an empty table from the extracted information and
   * returns that table and the number of chunks.
   * The header has the following format:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Chunk size            | ChunkOffset                           |   4
   * Chunk count           | ChunkID                               |   4
   * Column count          | ColumnID                              |   2
   * Column types          | TypeID array                          |   Column Count * 1
   * Column nullable       | bool (stored as BoolAsByteType)       |   Column Count * 1
   * Column name lengths   | size_t array                          |   Column Count * 1
   * Column names          | std::string array                     |   Sum of lengths of all names
   *
   */
  static std::pair<std::shared_ptr<Table>, ChunkID> _read_header(std::ifstream& file);

  /*
   * Creates a chunk from chunk information from the given file and adds it to the given table.
   * The chunk information has the following form:
   *
   * ----------------
   * |  Row count   |
   * |--------------|
   * |  Segments¹   |
   * ----------------
   *
   * ¹Number of columns is provided in the binary header
   */
  static void _import_chunk(std::ifstream& file, std::shared_ptr<Table>& table);

  // Calls the right _import_column<ColumnDataType> depending on the given data_type.
  static std::shared_ptr<BaseSegment> _import_segment(std::ifstream& file, ChunkOffset row_count, DataType data_type,
                                                      bool is_nullable);

  template <typename ColumnDataType>
  // Reads the column type from the given file and chooses a segment import function from it.
  static std::shared_ptr<BaseSegment> _import_segment(std::ifstream& file, ChunkOffset row_count, bool is_nullable);

  /*
   * Imports a serialized ValueSegment from the given file.
   * In case T is pmr_string the file contains:
   *
   * Description           | Type                                  | Size in byte
   * -----------------------------------------------------------------------------------------
   * Length of Strings     | size_t array                          |   row_count * 2
   * Values                | string array                          |   Total sum of string lengths
   *
   *
   * In case the column is nullable the file contains:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Is Value Null?        | bool (stored as BoolAsByteType)       |  row_count * 1
   * Values                | T                                     |  row_count * sizeof(T)
   *
   *
   * For all other cases the file contains:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Values                | T                                     |  row_count * sizeof(T)
   *
   */
  template <typename T>
  static std::shared_ptr<ValueSegment<T>> _import_value_segment(std::ifstream& file, ChunkOffset row_count,
                                                                bool is_nullable);

  /*
   * Imports a serialized DictionarySegment from the given file.
   * The file must contain data in the following format:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Width of attribute v. | AttributeVectorWidth                  |   1
   * Size of dictionary v. | ValueID                               |   4
   * Dictionary Values°    | T (int, float, double, long)          |   dict. size * sizeof(T)
   * Dict. String Length^  | size_t                                |   dict. size * 2
   * Dictionary Values^    | pmr_string                            |   Sum of all string lengths
   * Attribute v. values   | uintX                                 |   row_count * width of attribute v.
   *
   * ^: These fields are only needed if the type of the column is a string.
   * °: This field is needed if the type of the column is NOT a string
   */
  template <typename T>
  static std::shared_ptr<DictionarySegment<T>> _import_dictionary_segment(std::ifstream& file, ChunkOffset row_count);

  // Calls the _import_attribute_vector<uintX_t> function that corresponds to the given attribute_vector_width.
  static std::shared_ptr<BaseCompressedVector> _import_attribute_vector(std::ifstream& file, ChunkOffset row_count,
                                                                        AttributeVectorWidth attribute_vector_width);

  /*
   * Imports a serialized RunLengthSegment from the given file.
   * The file must contain data in the following format:
   *
   * Description            | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Run count              | uint32_t                              |   4
   * Values                 | T (int, float, double, long)          |   Run count * sizeof(T)
   * NULL values            | vector<bool> (BoolAsByteType)         |   Run count * 1
   * End Positions          | ChunkOffset                           |   Run count * 4
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   *
   */
  template <typename T>
  static std::shared_ptr<RunLengthSegment<T>> _import_run_length_segment(std::ifstream& file, ChunkOffset row_count);

  // Reads row_count many values from type T and returns them in a vector
  template <typename T>
  static pmr_vector<T> _read_values(std::ifstream& file, const size_t count);

  // Reads row_count many strings from input file. String lengths are encoded in type T.
  static pmr_vector<pmr_string> _read_string_values(std::ifstream& file, const size_t count);

  // Reads a single value of type T from the input file.
  template <typename T>
  static T _read_value(std::ifstream& file);

 private:
  // Name of the import file
  const std::string _filename;
  // Name for adding the table to the StorageManager
  const std::optional<std::string> _tablename;
};

}  // namespace opossum
