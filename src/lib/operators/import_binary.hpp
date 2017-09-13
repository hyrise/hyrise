#pragma once

#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_operator.hpp"
#include "abstract_read_only_operator.hpp"

#include "import_export/binary.hpp"
#include "storage/base_column.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "utils/assert.hpp"

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
  explicit ImportBinary(const std::string& filename, const optional<std::string> tablename = nullopt);

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

  // Returns the name of the operator
  const std::string name() const final;

  // This operator has no inputs.
  uint8_t num_in_tables() const final;

  // This operator has one table as output.
  uint8_t num_out_tables() const final;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override {
    Fail("Operator " + this->name() + " does not implement recreation.");
    return {};
  }

 private:
  /*
   * Reads the header from the given file.
   * Creates an empty table from the extracted information and
   * returns that table and the number of chunks.
   * The header has the following format:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Chunksize             | ChunkOffset                           |   4
   * Chunk count           | ChunkID                               |   4
   * Column count          | ColumnID                              |   2
   * Column types          | TypeID array                          |   Column Count * 1
   * Column name lengths   | ColumnNameLength array                |   Column Count * 1
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
   * |   Columns¹   |
   * ----------------
   *
   * ¹Number of columns is provided in the binary header
   */
  static Chunk _import_chunk(std::ifstream& file, std::shared_ptr<Table>& table);

  // Calls the right _import_column<DataType> depending on the given data_type.
  static std::shared_ptr<BaseColumn> _import_column(std::ifstream& file, ChunkOffset row_count,
                                                    const std::string& data_type);

  // Reads the column type from the given file and chooses a column import function from it.
  template <typename DataType>
  static std::shared_ptr<BaseColumn> _import_column(std::ifstream& file, ChunkOffset row_count);

  /*
   * Imports a serialized ValueColumn from the given file.
   * In case T is std::string the file contains:
   *
   * Description           | Type                                  | Size in byte
   * -----------------------------------------------------------------------------------------
   * Length of Strings     | StringLength array                    |   row_count * 2
   * Values                | std::string array                     |   Total sum of string lengths
   *
   * For all other cases the file contains:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Values                | T                                     |  row_count * sizeof(T)
   *
   */
  template <typename T>
  static std::shared_ptr<ValueColumn<T>> _import_value_column(std::ifstream& file, ChunkOffset row_count);

  /*
   * Imports a serialized DictionaryColumn from the given file.
   * The file must contain data in the folowing format:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Width of attribute v. | AttributeVectorWidth                  |   1
   * Size of dictionary v. | ValueID                               |   4
   * Dictionary Values°    | T (int, float, double, long)          |   dict. size * sizeof(T)
   * Dict. String Length^  | StringLength                          |   dict. size * 2
   * Dictionary Values^    | std::string                           |   Sum of all string lengths
   * Attribute v. values   | uintX                                 |   row_count * width of attribute v.
   *
   * ^: These fields are only needed if the type of the column is a string.
   * °: This field is needed if the type of the column is NOT a string
   */
  template <typename T>
  static std::shared_ptr<DictionaryColumn<T>> _import_dictionary_column(std::ifstream& file, ChunkOffset row_count);

  // Calls the _import_attribute_vector<uintX_t> function that corresponds to the given attribute_vector_width.
  static std::shared_ptr<BaseAttributeVector> _import_attribute_vector(std::ifstream& file, ChunkOffset row_count,
                                                                       AttributeVectorWidth attribute_vector_width);

  // Reads row_count many values from type T and returns them in a vector
  template <typename T>
  static pmr_vector<T> _read_values(std::ifstream& file, const size_t count);

  // Reads row_count many strings from input file. String lengths are encoded in type T.
  template <typename T = StringLength>
  static pmr_vector<std::string> _read_string_values(std::ifstream& file, const size_t count);

  // Reads a single value of type T from the input file.
  template <typename T>
  static T _read_value(std::ifstream& file);

 private:
  // Name of the import file
  const std::string _filename;
  // Name for adding the table to the StorageManager
  const optional<std::string> _tablename;
};

}  // namespace opossum
