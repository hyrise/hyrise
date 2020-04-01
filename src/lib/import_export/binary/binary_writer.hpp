#pragma once

#include <memory>
#include <string>
#include <vector>

#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "storage/lz4_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/value_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseCompressedVector;
enum class CompressedVectorType : uint8_t;

class BinaryWriter {
 public:
  static void write(const Table& table, const std::string& filename);

 private:
  /**
   * This methods writes the header of this table into the given ofstream.
   *
   * Description                 | Type                                | Size in bytes
   * --------------------------------------------------------------------------------------------------------
   * Chunk size                  | ChunkOffset                         | 4
   * Chunk count                 | ChunkID                             | 4
   * Column count                | ColumnID                            | 2
   * Column types                | TypeID array                        | Column Count * 1
   * Column nullable             | bool (stored as BoolAsByteType)     | Column Count * 1
   * Column name lengths         | size_t array                        | Column Count * 1
   * Column names                | std::string array                   | Sum of lengths of all names
   */
  static void _write_header(const Table& table, std::ofstream& ofstream);

  /**
   * Writes the contents of the chunk into the given ofstream.
   * First, it creates a chunk header with the following contents:
   *
   * Description                 | Type                                | Size in bytes
   * --------------------------------------------------------------------------------------------------------
   * Row count                   | ChunkOffset                         | 4
   *
   * Next, it dumps the contents of the segments in the respective format (depending on the type
   * of the segment, such as ValueSegment, ReferenceSegment, DictionarySegment, RunLengthSegment).
   */
  static void _write_chunk(const Table& table, std::ofstream& ofstream, const ChunkID& chunk_id);

  /**
   * ValueSegments are dumped with the following layout:
   *
   * Description                 | Type                                | Size in bytes
   * --------------------------------------------------------------------------------------------------------
   * Encoding Type               | EncodingType                        | 1
   * Null Values'                | vector<bool> (BoolAsByteType)       | Rows * 1
   * Values°                     | T (int, float, double, long)        | Rows * sizeof(T)
   * Length of Strings^          | vector<size_t>                      | Rows * 2
   * Values^                     | std::string                         | Rows * string.length()
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   *
   * ': These fields are only written if the column is nullable.
   * ^: These fields are only written if the type of the column IS a string.
   * °: This field is writen if the type of the column is NOT a string
   */
  template <typename T>
  static void _write_segment(const ValueSegment<T>& value_segment, std::ofstream& ofstream);

  /**
   * ReferenceSegments are dumped with the following layout, which is similar to value segments:
   *
   * Description                 | Type                                | Size in bytes
   * --------------------------------------------------------------------------------------------------------
   * Encoding Type               | EncodingType                        | 1
   * Values°                     | T (int, float, double, long)        | Rows * sizeof(T)
   * Length of Strings^          | vector<size_t>                      | Rows * 2
   * Values^                     | std::string                         | Rows * string.length()
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   *
   * ^: These fields are only written if the type of the column IS a string.
   * °: This field is writen if the type of the column is NOT a string
   */
  static void _write_segment(const ReferenceSegment& reference_segment, std::ofstream& ofstream);

  /**
   * DictionarySegments are dumped with the following layout:
   *
   * Description                 | Type                                | Size in bytes
   * --------------------------------------------------------------------------------------------------------
   * Encoding Type               | EncodingType                        | 1
   * Width of attribute vector   | AttributeVectorWidth                | 1
   * Size of dictionary vector   | ValueID                             | 4
   * Dictionary Values°          | T (int, float, double, long)        | Dictionary size * sizeof(T)
   * Dictionary String Length^   | size_t                              | Dictionary size * 2
   * Dictionary Values^          | std::string                         | Sum of all string lengths
   * Attribute vector values     | uintX                               | Rows * width of attribute vector
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   *
   * ^: These fields are only written if the type of the column IS a string.
   * °: This field is written if the type of the column is NOT a string
   */
  template <typename T>
  static void _write_segment(const DictionarySegment<T>& dictionary_segment, std::ofstream& ofstream);

  /**
   * FixedStringDictionarySegments are dumped with the following layout:
   *
   * Description                 | Type                                | Size in bytes
   * --------------------------------------------------------------------------------------------------------
   * Encoding Type               | EncodingType                        | 1
   * Width of attribute vector   | AttributeVectorWidth                | 1
   * Size of dictionary vector   | ValueID                             | 4
   * FixedString length          | uint32_t                            | 8
   * Dictionary Values           | char array                          | Dictionary size * FixedString length
   * Attribute vector values     | uintX                               | Rows * width of attribute vector
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   */
  template <typename T>
  static void _write_segment(const FixedStringDictionarySegment<T>& fixed_string_dictionary_segment,
                             std::ofstream& ofstream);

  /**
   * RunLengthSegments are dumped with the following layout:
   *
   * Description                 | Type                                | Size in bytes
   * --------------------------------------------------------------------------------------------------------
   * Encoding Type               | EncodingType                        | 1
   * Run count                   | uint32_t                            | 4
   * Values                      | T (int, float, double, long)        | Run count * sizeof(T)
   * NULL values                 | vector<bool> (BoolAsByteType)       | Run count * 1
   * End Positions               | ChunkOffset                         | Run count * 4
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   */
  template <typename T>
  static void _write_segment(const RunLengthSegment<T>& run_length_segment, std::ofstream& ofstream);

  /**
   * FrameOfReferenceSegments are dumped with the following layout:
   *
   * Description                 | Type                                | Size in bytes
   * --------------------------------------------------------------------------------------------------------
   * Encoding Type               | EncodingType                        | 1
   * Width of offset vector      | AttributeVectorWidth                | 1
   * Number of Blocks            | uint32_t                            | 4
   * Block minima                | T                                   | Number of blocks * sizeof(T)
   * Size                        | uint32_t                            | 4
   * NULL values                 | vector<bool> (BoolAsByteType)       | size * 1
   * Offset values               | uint32_t                            | size * 4
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   */
  template <typename T>
  static void _write_segment(const FrameOfReferenceSegment<T>& frame_of_reference_segment, std::ofstream& ofstream);

  /**
   * LZ4Segments are dumped with the following layout:
   *
   * Description                 | Type                                | Size in bytes
   * --------------------------------------------------------------------------------------------------------
   * Encoding Type               | EncodingType                        | 1
   * Number of Rows (in seg)     | uint32_t                            | 4
   * Number of Blocks            | uint32_t                            | 4
   * Block size¹                 | uint32_t                            | 4
   * Last Block size             | uint32_t                            | 4
   * lz4 Block sizes             | vector<uint32_t>                    | Number of blocks * 4
   * lz4 Blocks                  | vector<vector<char>>                | Sum(lz4 block sizes)
   * NULL values size            | uint32_t                            | 4
   * NULL values²                | vector<bool> (BoolAsByteType)       | Size * 1
   * Dictionary size             | uint32_t                            | 4
   * Dictionary                  | vector<char>                        | Dictionary size * 1
   * string offset size          | uint32_t                            | 4
   * string offset data size³    | uint32_t                            | 4
   * string offset³              | uint32_t                            | size * 4
   *
   * Please note that the number of rows are written in the header of the chunk.
   * The type of the column can be found in the global header of the file.
   *
   * ¹: This field is written if the number of blocks is greater than 1. Otherwise "Last Block Size" contains the
   *    size of the single block or 0 if there are no blocks
   * ²: This field is only written if NULL value size is not 0
   * ³: These fields are only written if string offset size is not 0
   */
  template <typename T>
  static void _write_segment(const LZ4Segment<T>& lz4_segment, std::ofstream& ofstream);

  template <typename T>
  static uint32_t _compressed_vector_width(const BaseEncodedSegment& base_encoded_segment);

  // Chooses the right Compressed Vector depending on the CompressedVectorType and exports it.
  static void _export_compressed_vector(std::ofstream& ofstream, const CompressedVectorType type,
                                        const BaseCompressedVector& compressed_vector);

  template <typename T>
  static size_t _size(const T& object);
};
}  // namespace opossum
