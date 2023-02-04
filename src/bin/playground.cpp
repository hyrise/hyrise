#include <fcntl.h>
#include <sys/mman.h>
#include <fstream>
#include <iostream>
#include <numeric>
#include <vector>
#include <span>
#include "types.hpp"

#include <unistd.h>

#include "storage/chunk_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"

using namespace hyrise;  // NOLINT

using chunk_prototype = std::vector<std::shared_ptr<ValueSegment<int>>>;
using dict_chunk_prototype = std::vector<std::shared_ptr<DictionarySegment<int>>>;

dict_chunk_prototype create_dictionary_segment_chunk(const uint32_t row_count, const uint32_t column_count) {
  /*
   * Create a chunk with index-times repeating elements in each segment.
   * Example: in segment 0 every value is unique, in segment 1 every value appears twice, in segment 2 thrice ...
   * Dictionary-encode each segment and return dictionary encoded chunk.
   */

  auto chunk = chunk_prototype{};
  auto dictionary_encoded_chunk = dict_chunk_prototype{};

  const auto num_values = int64_t{column_count * row_count};

  std::cout << "We create a dictionary-encoded chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << num_values << " values." << std::endl;

  chunk.reserve(column_count);
  for (auto segment_index = uint32_t{0}; segment_index < column_count; ++segment_index) {
    auto new_value_segment = std::make_shared<ValueSegment<int>>();

    auto current_value = int32_t{0};
    auto value_count = uint32_t{1}; //start 1-indexed to avoid issues with modulo operations

    while (value_count - 1 < row_count) { //as we start 1-indexed we need to adapt while-condition to create row-count many elements
      new_value_segment->append(current_value);

      //create segment-index many duplicates of each value in the segment
      if (value_count % (segment_index + 1) == 0) {
        current_value++;
      }
      value_count++;
    }
    chunk.emplace_back(new_value_segment);
  }

  dictionary_encoded_chunk.reserve(column_count);
  for (auto column_index = uint32_t{0}; column_index < column_count; ++column_index) {
    const auto segment =
      ChunkEncoder::encode_segment(chunk.at(column_index), DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
    const auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(segment);
    dictionary_encoded_chunk.emplace_back(dict_segment);
  }

  return dictionary_encoded_chunk;
}

int main() {
  std::cout << "Hello world!!" << std::endl;

  auto dict_chunk = create_dictionary_segment_chunk(100, 1);
  auto dictionary_segment = dict_chunk[0];
  auto dictionary_span = std::span{dictionary_segment->dictionary()->data(), dictionary_segment->dictionary()->size()};
  for (auto value : dictionary_span) {
    std::cout << value << std::endl;
  }
  auto attribute_vector = dictionary_segment->attribute_vector();
  auto data_pointer = attribute_vector->data_pointer();
  auto data = (pmr_vector<uint8_t>*) data_pointer;
  std::cout << data->size() << std::endl;
  auto data_vector = (uint8_t*) data->data();
  std::cout << data_vector[0] << std::endl;
  auto attribute_span = std::span{(uint8_t*) ((pmr_vector<uint8_t>*) dictionary_segment->attribute_vector()->data_pointer())->data(), dictionary_segment->attribute_vector()->data_size()};
  for (auto value : attribute_span) {
    std::cout << (uint32_t) value << std::endl;
  }
  return 0;
}
