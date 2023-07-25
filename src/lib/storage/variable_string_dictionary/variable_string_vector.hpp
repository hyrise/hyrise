#pragma once

#include "storage/variable_string_dictionary_segment.hpp"
#include "storage/variable_string_dictionary/variable_string_vector_iterator.hpp"

namespace hyrise {

class VariableStringVector {
 public:
  explicit VariableStringVector(const std::shared_ptr<const pmr_vector<char>>& dictionary, size_t size);

  VariableStringVectorIterator begin() const noexcept;
  VariableStringVectorIterator end() const noexcept;

  VariableStringVectorIterator cbegin() const noexcept {
    return begin();
  }
  VariableStringVectorIterator cend() const noexcept {
    return end();
  }
  size_t size() const;
 protected:
  std::shared_ptr<const pmr_vector<char>> _dictionary;
  size_t _size;
};

}  // namespace hyrise
