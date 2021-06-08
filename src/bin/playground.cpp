#include <iostream>
#include <iterator>
#include <algorithm>


#include "hyrise.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_segment.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  std::cout << "Hello world!!" << std::endl;

  const auto column_count = 2ul;
  const auto chunk_size = ChunkOffset{2'000};
  const auto row_count = size_t{40'000};

  const auto table_generator = std::make_shared<SyntheticTableGenerator>();

  auto table_a = table_generator->generate_table(column_count, row_count, chunk_size,
                                                 SegmentEncodingSpec{EncodingType::Dictionary});

  const auto column_id = ColumnID{0};
  auto all_segments_to_compare = std::vector<std::shared_ptr<DictionarySegment<int>>>();

  const auto chunk_count = table_a->chunk_count();
  all_segments_to_compare.reserve(chunk_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table_a->get_chunk(chunk_id);
    const auto segment = chunk->get_segment(column_id);
    const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(segment);
    if (dictionary_segment) {
      all_segments_to_compare.push_back(dictionary_segment);
    } else {
      std::cout << "not a DictionarySegment<int>";
    }
  }

  std::cout << "we found " << all_segments_to_compare.size() << " segments of type DictionarySegment<int>" << std::endl;
  for (auto segment_index = 1ul; segment_index < all_segments_to_compare.size(); ++segment_index) {
    const auto segment_a = all_segments_to_compare[segment_index - 1];
    const auto segment_b = all_segments_to_compare[segment_index];

    const auto dictionary_a = segment_a->dictionary();
    const auto dictionary_b = segment_b->dictionary();

    std::vector<int> v_intersection;
    std::set_intersection(dictionary_a->begin(), dictionary_a->end(),
                          dictionary_b->begin(), dictionary_b->end(),
                          std::back_inserter(v_intersection));
    const auto intersection_size = v_intersection.size();
    const auto union_size = dictionary_a->size() + dictionary_b->size() - intersection_size;
    const auto jaccard_index = intersection_size * 1.0 / union_size; 
    
    std::cout << "Segment[" << (segment_index-1)<< "] vs Segment[" << segment_index << "]" << " -> jaccard index=" << jaccard_index << std::endl;
  }
  return 0;
}
