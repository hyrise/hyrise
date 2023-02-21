#include <memory>

#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/table.hpp"

namespace hyrise {

class StorageManagerTestUtil {
 public:
  static std::shared_ptr<Chunk> create_dictionary_segment_chunk(const uint32_t row_count, const uint32_t column_count) {
    /*
   * Create a chunk with index-times repeating elements in each segment.
   * Example: in segment 0 every value is unique, in segment 1 every value appears twice, in segment 2 thrice ...
   * Dictionary-encode each segment and return dictionary encoded chunk.
   */
    auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
    for (auto segment_index = uint32_t{0}; segment_index < column_count; ++segment_index) {
      auto new_value_segment = std::make_shared<ValueSegment<int32_t>>();

      auto current_value = static_cast<int32_t>(row_count);
      auto value_count = uint32_t{1};  //start 1-indexed to avoid issues with modulo operations
      while (value_count - 1 <
             row_count) {  //as we start 1-indexed we need to adapt while-condition to create row-count many elements
        new_value_segment->append(current_value);

        //create segment-index many duplicates of each value in the segment
        if (value_count % (segment_index + 1) == 0) {
          --current_value;
        }
        ++value_count;
      }

      auto ds_int =
          ChunkEncoder::encode_segment(new_value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
      segments.emplace_back(ds_int);
    }

    const auto dictionary_encoded_chunk = std::make_shared<Chunk>(segments);
    return dictionary_encoded_chunk;
  }

  static std::shared_ptr<Chunk> create_dictionary_segment_chunk_large(const uint32_t row_count,
                                                                      const uint32_t column_count) {
    /*
   * Create a chunk with index-times repeating elements in each segment.
   * Example: in segment 0 every value is unique, in segment 1 every value appears twice, in segment 2 thrice ...
   * Dictionary-encode each segment and return dictionary encoded chunk.
   */
    auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
    for (auto segment_index = uint32_t{0}; segment_index < column_count; ++segment_index) {
      auto new_value_segment = std::make_shared<ValueSegment<int32_t>>(false, ChunkOffset{row_count});

      auto current_value = static_cast<int32_t>(row_count);
      auto value_count = uint32_t{1};  //start 1-indexed to avoid issues with modulo operations
      while (value_count - 1 <
             row_count) {  //as we start 1-indexed we need to adapt while-condition to create row-count many elements
        new_value_segment->append(current_value);
        --current_value;
        ++value_count;
      }

      auto encoded_segment =
          ChunkEncoder::encode_segment(new_value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
      segments.emplace_back(encoded_segment);
    }

    const auto dictionary_encoded_chunk = std::make_shared<Chunk>(segments);
    return dictionary_encoded_chunk;
  }
};

}  // namespace hyrise
