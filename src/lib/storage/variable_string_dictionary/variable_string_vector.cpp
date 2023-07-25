#include "variable_string_vector.hpp"

namespace hyrise {

hyrise::VariableStringVector::VariableStringVector(const std::shared_ptr<const pmr_vector<char>>& dictionary,
                                                   size_t size)
    : _dictionary{dictionary}, _size{size} {}

VariableStringVectorIterator VariableStringVector::begin() const noexcept {
  return VariableStringVectorIterator(_dictionary, 0);
}

VariableStringVectorIterator VariableStringVector::end() const noexcept {
  return VariableStringVectorIterator(_dictionary, _dictionary->size());
}

size_t VariableStringVector::size() const {
  return _size;
}
}  // namespace hyrise
