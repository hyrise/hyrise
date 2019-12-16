#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/value_segment.hpp"
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

  static void write_binary(const Table& table, const std::string& filename);

  /**
   * Executes the export operator
   * @return The table that was also the input
   */
  std::shared_ptr<const Table> _on_execute() final;

  /**
   * Name of the operator is ExportBinary
   */
  const std::string& name() const final;

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
  static void _write_header(const Table& table, std::ofstream& ofstream);

  /**
   * Writes the contents of the chunk into the given ofstream.
   * First, it creates a chunk header with the following contents:
   *
   * Description           | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Row count             | ChunkOffset                           |  4
   *
   * Next, it dumps the contents of the segments in the respective format (depending on the type
   * of the segment, such as ValueSegment, ReferenceSegment, DictionarySegment, RunLengthSegment).
   *
   * @param table The table we are currently exporting
   * @param ofstream The output stream to write to
   * @param chunkId The id of the chunk that is to be worked on now
   *
   */
  static void _write_chunk(const Table& table, std::ofstream& ofstream, const ChunkID& chunk_id);

  [[noreturn]] static void _write_segment(const BaseSegment& base_segment, std::ofstream& ofstream);

  /**
   * Value Segments are dumped with the following layout:
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
   * @param value_segment The segment to export
   * @param ofstream The output stream for exporting
   *
   */
  template <typename T>
  static void _write_segment(const ValueSegment<T>& value_segment, std::ofstream& ofstream);

  /**
   * Reference Segments are dumped with the following layout, which is similar to value segments:
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
   * @param reference_segment The segment to export
   * @param base_context A context in the form of an ExportContext. Contains a reference to the ofstream.
   */
  static void _write_segment(const ReferenceSegment& reference_segment, std::ofstream& ofstream);

  /**
   * Dictionary Segments are dumped with the following layout:
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
   * @param base_dictionary_segment The segment to export
   * @param ofstream The output stream for exporting
   */
  template <typename T>
  static void _write_segment(const DictionarySegment<T>& dictionary_segment, std::ofstream& ofstream);

  /**
   * RunLength Segments are dumped with the following layout:
   *
   * Description            | Type                                  | Size in bytes
   * -----------------------------------------------------------------------------------------
   * Column Type            | ColumnType                            |   1
   * Run count              | uint32_t                              |   4
   * Values                 | T (int, float, double, long)          |   Run count * sizeof(T)
   * NULL values            | vector<bool> (BoolAsByteType)         |   Run count * 1
   * End Positions          | ChunkOffset                           |   Run count * 4
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   *
   *
   * @param run_length_segment The segment to export
   * @param ofstream The output stream for exporting
   */
  template <typename T>
  static void _write_segment(const RunLengthSegment<T>& run_length_segment, std::ofstream& ofstream);

  // Chooses the right FixedSizeByteAlignedVector depending on the attribute_vector_width and exports it.
  static void _export_attribute_vector(std::ofstream& ofstream, const CompressedVectorType type,
                                       const BaseCompressedVector& attribute_vector);

  template <typename T>
  static size_t _size(const T& object);
};
}  // namespace opossum
