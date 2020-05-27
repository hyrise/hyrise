#pragma once

#include "resolve_type.hpp"
#include "storage/lz4_segment.hpp"

namespace opossum {

class LZ4Compare {
 public:
  inline static int WIDTH = 15;

  static void compare_segments(const std::shared_ptr<const BaseSegment>& left,
                               const std::shared_ptr<const BaseSegment>& right) {
    resolve_data_and_segment_type(*left, [&](const auto data_type_t1, const auto& resolved_segment_left) {
      resolve_data_and_segment_type(*right, [&](const auto data_type_t2, const auto& resolved_segment_right) {
        _compare(resolved_segment_left, resolved_segment_right);
      });
    });
  }

  template <typename T>
  static void _compare(const LZ4Segment<T>& left, const LZ4Segment<T>& right) {
    const auto& headers = {"left", "right"};

    std::cout << std::setw(WIDTH) << "metric" << std::setw(0) << " |";
    for (const auto& header : headers) {
      std::cout << std::setw(WIDTH) << header;
    }
    std::cout << std::endl;

    for (int i = 0; i < 3 * WIDTH + 2; ++i) {
    	std::cout << "-";
    }
    std::cout << std::endl;

    std::cout << std::setw(WIDTH) << "#blocks" << std::setw(0) << " |" << std::setw(WIDTH);
    std::cout << left._lz4_blocks.size() << std::setw(WIDTH) << right._lz4_blocks.size();
    std::cout << std::endl;

    std::cout << std::setw(WIDTH) << "#nulls" << std::setw(0) << " |" << std::setw(WIDTH);
    const auto& nulls_left = left._null_values;
    const std::string num_nulls_left = nulls_left ? std::to_string(nulls_left.value().size()) : "-";
    const auto& nulls_right = right._null_values;
    const std::string num_nulls_right = nulls_right ? std::to_string(nulls_right.value().size()) : "-";

    std::cout << num_nulls_left << std::setw(WIDTH) << num_nulls_right;
    std::cout << std::endl;

    std::cout << std::setw(WIDTH) << "block size" << std::setw(0) << " |" << std::setw(WIDTH);
    std::cout << left._block_size << std::setw(WIDTH) << right._block_size;
    std::cout << std::endl;

    std::cout << std::setw(WIDTH) << "first b.s." << std::setw(0) << " |" << std::setw(WIDTH);
    std::cout << left._lz4_blocks.at(0).size() << std::setw(WIDTH) << right._lz4_blocks.at(0).size();
    std::cout << std::endl;

    std::cout << std::setw(WIDTH) << "last b.s." << std::setw(0) << " |" << std::setw(WIDTH);
    std::cout << left._last_block_size << std::setw(WIDTH) << right._last_block_size;
    std::cout << std::endl;

    std::cout << std::setw(WIDTH) << "compressed s." << std::setw(0) << " |" << std::setw(WIDTH);
    std::cout << left._compressed_size << std::setw(WIDTH) << right._compressed_size;
    std::cout << std::endl;

    std::cout << std::setw(WIDTH) << "#elements" << std::setw(0) << " |" << std::setw(WIDTH);
    std::cout << left._num_elements << std::setw(WIDTH) << right._num_elements;
    std::cout << std::endl << std::endl << std::endl;

    /* members of LZ4segment
  const pmr_vector<pmr_vector<char>> _lz4_blocks;
  const std::optional<pmr_vector<bool>> _null_values;
  const pmr_vector<char> _dictionary;
  const std::optional<std::unique_ptr<const BaseCompressedVector>> _string_offsets;
  const size_t _block_size;
  const size_t _last_block_size;
  const size_t _compressed_size;
  const size_t _num_elements;
  */
  }

  static void _compare(const BaseSegment& left, const BaseSegment& right) {
    std::cout << "not implemented" << std::endl;
  }
};

}  // namespace opossum
