#pragma once

namespace hyrise {

using DereferenceValue = std::string_view;

class VariableStringVectorIterator : public boost::iterator_facade<VariableStringVectorIterator, DereferenceValue,
                                                                std::random_access_iterator_tag, DereferenceValue> {
 public:
  explicit VariableStringVectorIterator(const std::shared_ptr<const pmr_vector<char>>& dictionary,
                                        uint32_t current_offset)
      : _dictionary{dictionary}, _current_offset{current_offset} {}
 protected:
  friend class boost::iterator_core_access;

  // We have a couple of NOLINTs here becaues the facade expects these method names:

  bool equal(VariableStringVectorIterator const& other) const {  // NOLINT
    return _dictionary == other._dictionary && _current_offset == other._current_offset;
  }

  size_t distance_to(VariableStringVectorIterator const& other) const {  // NOLINT
    auto strings_in_between = size_t{0};
    for (auto offset = _current_offset; offset < other._current_offset; ++offset) {
      if (_dictionary->operator[](offset) == '\0') {
        ++strings_in_between;
      }
    }
    return strings_in_between;
  }

  void advance(size_t n) {  // NOLINT
    for (auto count = size_t{0}; count < n; ++count) {
      increment();
    }
  }

  void increment() {  // NOLINT
    auto offset = _current_offset;
    // We assume that we do not overrun the end of the vector.
    for (;; ++offset) {
      if (_dictionary->operator[](offset) == '\0') {
        break;
      }
    }
    // +1 due to NULL byte.
    _current_offset = offset + 1;
  }

  void decrement() {  // NOLINT
    // -2 because we are pointing to the first character of a string.
    // The next character in front will be a null byte, so we have to skip it.
    auto offset = _current_offset - 2;
    // We assume that we do not overrun the end of the vector.
    for (;; --offset) {
      if (_dictionary->operator[](offset) == '\0') {
        break;
      }
    }
    // +1 due to NULL byte.
    _current_offset = offset + 1;
  }

  const std::string_view dereference() const {  // NOLINT
    return std::string_view{_dictionary->data() + _current_offset, strlen(_dictionary->data() + _current_offset)};
  }

  std::shared_ptr<const pmr_vector<char>> _dictionary;
  uint32_t _current_offset;
};

}  // namespace hyrise
