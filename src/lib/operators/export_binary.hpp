#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "import_export/binary.hpp"
#include "storage/abstract_column_visitor.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseCompressedVector;
enum class CompressedVectorType : uint8_t;

/**
 * Note: ExportBinary does not support null values at the moment
 */
class ExportBinary : public AbstractReadOnlyOperator {
 public:
  explicit ExportBinary(const std::shared_ptr<const AbstractOperator>& in, const std::string& filename);

  /**
   * Executes the export operator
   * @return The table that was also the input
   */
  std::shared_ptr<const Table> _on_execute() final;

  /**
   * Name of the operator is ExportBinary
   */
  const std::string name() const final;

 protected:
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  // Path of the binary file
  const std::string _filename;

  /**
   * This methods writes the header of this table into the given ofstream.
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
   * @param table The table that is to be exported
   * @param ofstream The output stream for exporting
   */
  static void _write_header(const std::shared_ptr<const Table>& table, std::ofstream& ofstream);

  /**
   * Writes the contents of the chunk into the given ofstream.
   * First, it creates a chunk header with the following contents:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Row count             | ChunkOffset                           |  4
   *
   * Next, it dumps the contents of the columns in the respective format (depending on the type
   * of the column, such as ReferenceColumn, DictionaryColumn, ValueColumn).
   *
   * @param table The table we are currently exporting
   * @param ofstream The output stream to write to
   * @param chunkId The id of the chunk that is to be worked on now
   *
   */
  static void _write_chunk(const std::shared_ptr<const Table>& table, std::ofstream& ofstream, const ChunkID& chunk_id);

  template <typename T>
  class ExportBinaryVisitor;

  struct ExportContext : ColumnVisitorContext {
    explicit ExportContext(std::ofstream& ofstream) : ofstream(ofstream) {}
    std::ofstream& ofstream;
  };
};

template <typename T>
class ExportBinary::ExportBinaryVisitor : public AbstractColumnVisitor {
  /**
   * Value Columns are dumped with the following layout:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Column Type           | ColumnType                            |   1
   * Null Values'          | vector<bool> (BoolAsByteType)         |   rows * 1
   * Values°               | T (int, float, double, long)          |   rows * sizeof(T)
   * Length of Strings^    | vector<size_t>                        |   rows * 2
   * Values^               | std::string                           |   rows * string.length()
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   *
   * ': These fields are only written if the column is nullable.
   * ^: These fields are only written if the type of the column IS a string.
   * °: This field is writen if the type of the column is NOT a string
   *
   * @param base_column The Column to export
   * @param base_context A context in the form of an ExportContext. Contains a reference to the ofstream.
   *
   */
  void handle_column(const BaseValueColumn& base_column, std::shared_ptr<ColumnVisitorContext> base_context) final;

  /**
   * Reference Columns are dumped with the following layout, which is similar to value columns:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Column Type           | ColumnType                            |   1
   * Values°               | T (int, float, double, long)          |   rows * sizeof(T)
   * Length of Strings^    | vector<size_t>                        |   rows * 2
   * Values^               | std::string                           |   rows * string.length()
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   *
   * ^: These fields are only written if the type of the column IS a string.
   * °: This field is writen if the type of the column is NOT a string
   *
   * @param base_column The Column to export
   * @param base_context A context in the form of an ExportContext. Contains a reference to the ofstream.
   */
  void handle_column(const ReferenceColumn& ref_column, std::shared_ptr<ColumnVisitorContext> base_context) override;

  /**
   * Dictionary Columns are dumped with the following layout:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Column Type           | ColumnType                            |   1
   * Width of attribute v. | AttributeVectorWidth                  |   1
   * Size of dictionary v. | ValueID                               |   4
   * Dictionary Values°    | T (int, float, double, long)          |   dict. size * sizeof(T)
   * Dict. String Length^  | size_t                                |   dict. size * 2
   * Dictionary Values^    | std::string                           |   Sum of all string lengths
   * Attribute v. values   | uintX                                 |   rows * width of attribute v.
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   *
   * ^: These fields are only written if the type of the column IS a string.
   * °: This field is written if the type of the column is NOT a string
   *
   * @param base_column The Column to export
   * @param base_context A context in the form of an ExportContext. Contains a reference to the ofstream.
   */
  void handle_column(const BaseDictionaryColumn& base_column,
                     std::shared_ptr<ColumnVisitorContext> base_context) override;

  void handle_column(const BaseEncodedColumn& base_column, std::shared_ptr<ColumnVisitorContext> base_context) override;

 private:
  // Chooses the right FixedSizeByteAlignedVector depending on the attribute_vector_width and exports it.
  static void _export_attribute_vector(std::ofstream& ofstream, const CompressedVectorType type,
                                       const BaseCompressedVector& attribute_vector);
};
}  // namespace opossum
