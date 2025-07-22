#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "storage/variable_string_dictionary_segment.hpp"
#include "types.hpp"

namespace hyrise {

class VariableStringVectorIterator;

class VariableStringVector {
 public:
  explicit VariableStringVector(const pmr_vector<char>& dictionary, const pmr_vector<uint32_t>& offset_vector);

  VariableStringVectorIterator begin() const noexcept;
  VariableStringVectorIterator end() const noexcept;

  VariableStringVectorIterator cbegin() const noexcept;
  VariableStringVectorIterator cend() const noexcept;

  size_t size() const;

 protected:
  const pmr_vector<char>& _dictionary;
  const pmr_vector<uint32_t>& _offset_vector;
};

}  // namespace hyrise
