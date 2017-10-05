#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

#include "import_export/binary.hpp"
#include "storage/base_column.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/fitted_attribute_vector.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "utils/assert.hpp"

namespace opossum {

/**
 * Note: ExportBinary does not support null values at the moment
 */
class ExportBinary : public AbstractReadOnlyOperator {
 public:
  explicit ExportBinary(const std::shared_ptr<const AbstractOperator> in, const std::string& filename);

  /**
   * Executes the export operator
   * @return The table that was also the input
   */
  std::shared_ptr<const Table> _on_execute() final;

  /**
   * Name of the operator is ExportBinary
   */
  const std::string name() const final;

  /**
   * This operator allows one table as input
   */
  uint8_t num_in_tables() const final;

  /**
   * This operator has one table as output.
   */
  uint8_t num_out_tables() const final;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override {
    Fail("Operator " + this->name() + " does not implement recreation.");
    return {};
  }

 private:
  // Path of the binary file
  const std::string _filename;

  /**
   * This methods writes the header of this table into the given ofstream.
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
   * @param table The table that is to be exported
   * @param ofstream The outputstream for exporting
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
   * @param ofstream The outputstream to write to
   * @param chunkId The id of the chunk that is to be worked on now
   *
   */
  static void _write_chunk(const std::shared_ptr<const Table>& table, std::ofstream& ofstream, const ChunkID& chunkId);

  template <typename T>
  class ExportBinaryVisitor;

  struct ExportContext : ColumnVisitableContext {
    explicit ExportContext(std::ofstream& ofstream) : ofstream(ofstream) {}
    std::ofstream& ofstream;
  };
};

template <typename T>
class ExportBinary::ExportBinaryVisitor : public ColumnVisitable {
  /**
   * Value Columns are dumped with the following layout:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Column Type           | ColumnType                            |   1
   * Values°               | T (int, float, double, long)          |   rows * sizeof(T)
   * Length of Strings^    | vector<StringLength>                  |   rows * 2
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
   *
   */
  void handle_value_column(BaseColumn& base_column, std::shared_ptr<ColumnVisitableContext> base_context) final;

  /**
   * Reference Columns are dumped with the following layout, which is similar to value columns:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Column Type           | ColumnType                            |   1
   * Values°               | T (int, float, double, long)          |   rows * sizeof(T)
   * Length of Strings^    | vector<StringLength>                  |   rows * 2
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
  void handle_reference_column(ReferenceColumn& ref_column,
                               std::shared_ptr<ColumnVisitableContext> base_context) override;
  /**
   * Dictionary Columns are dumped with the following layout:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Column Type           | ColumnType                            |   1
   * Width of attribute v. | AttributeVectorWidth                  |   1
   * Size of dictionary v. | ValueID                               |   4
   * Dictionary Values°    | T (int, float, double, long)          |   dict. size * sizeof(T)
   * Dict. String Length^  | StringLength                          |   dict. size * 2
   * Dictionary Values^    | std::string                           |   Sum of all string lengths
   * Attribute v. values   | uintX                                 |   rows * width of attribute v.
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
  void handle_dictionary_column(BaseColumn& base_column, std::shared_ptr<ColumnVisitableContext> base_context) override;

 private:
  // Chooses the right FittedAttributeVector depending on the attribute_vector_width and exports it.
  static void _export_attribute_vector(std::ofstream& ofstream, const BaseAttributeVector& attribute_vector);
};
}  // namespace opossum
