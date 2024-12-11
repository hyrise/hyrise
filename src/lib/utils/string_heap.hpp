#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace hyrise {

class StringHeap : public Noncopyable {
 public:
  StringHeap() {}

  template<typename T>
  std::string_view add_string(const T& value) {
    auto block_size = BLOCK_SIZE;
    const auto string_size = value.size();
    if (_remaining_chars_in_last_block < string_size) {
      block_size = std::max(BLOCK_SIZE, string_size);
      _blocks.emplace_back(std::make_unique_for_overwrite<char[]>(block_size));
      _remaining_chars_in_last_block = block_size;
    }

    auto* heap_string_start = _blocks.back().get() + (block_size - _remaining_chars_in_last_block);
    value.copy(heap_string_start, string_size);
    _remaining_chars_in_last_block -= string_size;

    return std::string_view{heap_string_start, string_size};
  }

  size_t block_count() {
    return _blocks.size();
  }

 private:
  static constexpr auto BLOCK_SIZE = size_t{65'536};
  std::vector<std::unique_ptr<char[]>> _blocks;
  size_t _remaining_chars_in_last_block{0};
};

}  // namespace hyrise
