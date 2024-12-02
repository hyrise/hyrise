#include "variable_string_vector.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>

#include "storage/variable_string_dictionary/variable_string_vector_iterator.hpp"
#include "types.hpp"

namespace hyrise {

hyrise::VariableStringVector::VariableStringVector(const pmr_vector<char>& dictionary,
                                                   const pmr_vector<uint32_t>& offset_vector)
    : _dictionary{dictionary}, _offset_vector{offset_vector} {}

VariableStringVectorIterator VariableStringVector::begin() const noexcept {
  return VariableStringVectorIterator(_dictionary, _offset_vector, ValueID{0});
}

VariableStringVectorIterator VariableStringVector::end() const noexcept {
  return VariableStringVectorIterator(_dictionary, _offset_vector, ValueID{static_cast<ValueID::base_type>(size())});
}

VariableStringVectorIterator VariableStringVector::cbegin() const noexcept {
  return begin();
}

VariableStringVectorIterator VariableStringVector::cend() const noexcept {
  return end();
}

size_t VariableStringVector::size() const {
  return _offset_vector.size();
}
}  // namespace hyrise
