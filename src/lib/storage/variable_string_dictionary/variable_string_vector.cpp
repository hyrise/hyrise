#include "variable_string_vector.hpp"

namespace hyrise {

hyrise::VariableStringVector::VariableStringVector(const std::shared_ptr<const pmr_vector<char>>& dictionary,
                                                   const std::shared_ptr<const pmr_vector<uint32_t>>& offset_vector)
    : _dictionary{dictionary}, _offset_vector{offset_vector} {}

VariableStringVectorIterator VariableStringVector::begin() const noexcept {
  return VariableStringVectorIterator(_dictionary, _offset_vector, ValueID{0});
}

VariableStringVectorIterator VariableStringVector::end() const noexcept {
  return VariableStringVectorIterator(_dictionary, _offset_vector, ValueID(size()));
}

size_t VariableStringVector::size() const {
  return _offset_vector->size();
}
}  // namespace hyrise
