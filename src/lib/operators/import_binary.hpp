#pragma once

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "import_export/binary.hpp"
#include "storage/base_column.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/value_column.hpp"

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
  explicit ImportBinary(const std::string& filename, const std::optional<std::string> tablename = std::nullopt);

  /*
   * Reads the given binary file. The file must be in the following form:
   *
   * -----------------
   * |     Header    |
   * |---------------|
   * |     Chunks¹   |
   * |---------------|
   * |  Part. Header²|
   * |---------------|
   * |   Partitions² |
   * -----------------
   *
   * ¹ Zero or more chunks
   * ² Partition header is optional. If present, there is at least one partition.
   */
  std::shared_ptr<const Table> _on_execute() final;

  // Returns the name of the operator
  const std::string name() const final;

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
  static std::shared_ptr<Chunk> _import_chunk(std::ifstream& file, std::shared_ptr<Table>& table);

  /*
   * Reads the header from the given file.
   * Creates an empty table from the extracted information and
   * returns that table and the number of chunks.
   * The header has the following format:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Partition schema      | uint8_t                               |   1
   * Partition count       | PartitionID                           |   2
   * Partition specific    | ?                                     |   ?
   * 
   * The partition specific information consists of the following:
   * 
   * NullPartitioningSchema: empty
   * RoundRobinPartitioningSchema: empty
   * 
   * 
   * RangePartitioningSchema:
   * 
   * Description            | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Column to partition by | ColumnID                              |   4
   * DataType of bounds     | std::string array                     |   Length of DataType string representation
   * Bounds                 | Typed array                           |   (Partition count - 1) * x
   * 
   * 
   * HashPartitioningSchema:
   * 
   * Description            | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Column to partition by | ColumnID                              |   4
   *
   */
  static std::shared_ptr<AbstractPartitionSchema> _read_partitioning_header(std::ifstream& file);

  // Reads the partition info from the given file and adds the corresponding chunks to it
  static void _import_partition(std::ifstream& file, const std::shared_ptr<AbstractPartitionSchema>& partition_schema,
                                const PartitionID partition_id, std::map<ChunkID, PartitionID>& chunk_to_partition);

  // Calls the right _import_column<ColumnDataType> depending on the given data_type.
  static std::shared_ptr<BaseColumn> _import_column(std::ifstream& file, ChunkOffset row_count, DataType data_type,
                                                    bool is_nullable);

  // Reads the column type from the given file and chooses a column import function from it.
  template <typename ColumnDataType>
  static std::shared_ptr<BaseColumn> _import_column(std::ifstream& file, ChunkOffset row_count, bool is_nullable);

  /*
   * Imports a serialized ValueColumn from the given file.
   * In case T is std::string the file contains:
   *
   * Description           | Type                                  | Size in byte
   * -----------------------------------------------------------------------------------------
   * Length of Strings     | StringLength array                    |   row_count * 2
   * Values                | std::string array                     |   Total sum of string lengths
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
  static std::shared_ptr<ValueColumn<T>> _import_value_column(std::ifstream& file, ChunkOffset row_count,
                                                              bool is_nullable);

  /*
   * Imports a serialized DeprecatedDictionaryColumn from the given file.
   * The file must contain data in the following format:
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
  static std::shared_ptr<DictionaryColumn<T>> _import_dictionary_column(std::ifstream& file,
                                                                        ChunkOffset row_count);

  // Calls the _import_attribute_vector<uintX_t> function that corresponds to the given attribute_vector_width.
  static std::shared_ptr<BaseCompressedVector> _import_attribute_vector(std::ifstream& file, ChunkOffset row_count,
                                                                        AttributeVectorWidth attribute_vector_width);

  // Reads row_count many values from type T and returns them in a vector
  template <typename T>
  static pmr_vector<T> _read_values(std::ifstream& file, const size_t count);

  // Reads row_count many strings from input file. String lengths are encoded in type T.
  template <typename T = StringLength>
  static pmr_vector<std::string> _read_string_values(std::ifstream& file, const size_t count);

  static std::vector<AllTypeVariant> _read_all_type_variants(std::ifstream& file, const size_t count);

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
