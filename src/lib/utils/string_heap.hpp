#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "utils/small_prefix_string_view.hpp"

namespace hyrise {

class StringHeap : public Noncopyable {
 public:
  static constexpr auto BLOCK_SIZE = size_t{4'096};

  StringHeap() {}

  template<typename T>
  SmallPrefixStringView add_string(const T& value) {
    // std::cerr << "Start. _remaining_chars_in_last_block left: " << _remaining_chars_in_last_block << std::endl;
    const auto string_size = value.size();
    if (!SmallPrefixStringView::string_needs_heap_storage(string_size)) {
      // SmallPrefixStringView can store string directly. No reason to put string on heap.
      return SmallPrefixStringView{value};
    }

    const auto string_size_with_null_byte = string_size + 1;
    auto block_size = BLOCK_SIZE;
    if (_remaining_chars_in_last_block < string_size_with_null_byte) {
      block_size = std::max(BLOCK_SIZE, string_size_with_null_byte);
      _blocks.emplace_back(std::make_unique_for_overwrite<char[]>(block_size));
      _remaining_chars_in_last_block = block_size;
      // std::cerr << "Middle. _remaining_chars_in_last_block left: " << _remaining_chars_in_last_block << std::endl;
    }

    auto* heap_string_start = _blocks.back().get() + (block_size - _remaining_chars_in_last_block);
    value.copy(heap_string_start, string_size);
    *(heap_string_start + string_size) = '\0';
    _remaining_chars_in_last_block -= (string_size_with_null_byte);
    DebugAssert(value == std::string_view{heap_string_start}, "Created heap string is not equal to passed value.");

    // std::cerr << "End. _remaining_chars_in_last_block left: " << _remaining_chars_in_last_block << std::endl;
    return SmallPrefixStringView{heap_string_start, string_size};
  }

  size_t block_count() {
    return _blocks.size();
  }

 private:
  std::vector<std::unique_ptr<char[]>> _blocks;
  size_t _remaining_chars_in_last_block{0};
};

}  // namespace hyrise
