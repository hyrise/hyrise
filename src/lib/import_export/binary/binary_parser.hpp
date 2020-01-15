#pragma once

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "storage/base_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "storage/lz4_segment.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

/*
 * This parser reads an Opossum binary file and creates a table from that input.
 * Documentation of the file formats can be found in BinaryWriter header file.
 */
class BinaryParser {
 public:
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
  static std::shared_ptr<Table> parse(const std::string& filename);

 private:
  /*
   * Reads the header from the given file.
   * Creates an empty table from the extracted information and
   * returns that table and the number of chunks.
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

  template <typename T>
  static std::shared_ptr<ValueSegment<T>> _import_value_segment(std::ifstream& file, ChunkOffset row_count,
                                                                bool is_nullable);
  template <typename T>
  static std::shared_ptr<DictionarySegment<T>> _import_dictionary_segment(std::ifstream& file, ChunkOffset row_count);

  template <typename T>
  static std::shared_ptr<RunLengthSegment<T>> _import_run_length_segment(std::ifstream& file, ChunkOffset row_count);

  template <typename T>
  static std::shared_ptr<FrameOfReferenceSegment<T>> _import_frame_of_reference_segment(std::ifstream& file,
                                                                                        ChunkOffset row_count);
  template <typename T>
  static std::shared_ptr<LZ4Segment<T>> _import_lz4_segment(std::ifstream& file, ChunkOffset row_count);

  // Calls the _import_attribute_vector<uintX_t> function that corresponds to the given attribute_vector_width.
  static std::shared_ptr<BaseCompressedVector> _import_attribute_vector(std::ifstream& file, ChunkOffset row_count,
                                                                        AttributeVectorWidth attribute_vector_width);

  static std::unique_ptr<const BaseCompressedVector> _import_offset_value_vector(
      std::ifstream& file, ChunkOffset row_count, AttributeVectorWidth attribute_vector_width);

  // Reads row_count many values from type T and returns them in a vector
  template <typename T>
  static pmr_vector<T> _read_values(std::ifstream& file, const size_t count);

  // Reads row_count many strings from input file. String lengths are encoded in type T.
  static pmr_vector<pmr_string> _read_string_values(std::ifstream& file, const size_t count);

  // Reads a single value of type T from the input file.
  template <typename T>
  static T _read_value(std::ifstream& file);
};

}  // namespace opossum
