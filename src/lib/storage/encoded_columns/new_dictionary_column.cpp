#include "new_dictionary_column.hpp"

#include <memory>
#include <string>

#include "storage/column_visitable.hpp"
#include "storage/null_suppression/base_ns_vector.hpp"
#include "storage/value_column.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
NewDictionaryColumn<T>::NewDictionaryColumn(const std::shared_ptr<const pmr_vector<T>>& dictionary,
                                            const std::shared_ptr<const BaseNsVector>& attribute_vector,
                                            const ValueID null_value_id)
    : _dictionary{dictionary}, _attribute_vector{attribute_vector}, _null_value_id{null_value_id} {}

template <typename T>
const AllTypeVariant NewDictionaryColumn<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  auto decoder = _attribute_vector->create_base_decoder();
  const auto value_id = decoder->get(chunk_offset);

  if (value_id == _null_value_id) {
    return NULL_VALUE;
  }

  return (*_dictionary)[value_id];
}

template <typename T>
std::shared_ptr<const pmr_vector<T>> NewDictionaryColumn<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
ValueID NewDictionaryColumn<T>::null_value_id() const {
  return _null_value_id;
}

template <typename T>
size_t NewDictionaryColumn<T>::size() const {
  return _attribute_vector->size();
}

template <typename T>
void NewDictionaryColumn<T>::write_string_representation(std::string& row_string,
                                                         const ChunkOffset chunk_offset) const {
  PerformanceWarning("NewDictionaryColumn<T>::write_string_representation is potentially very slow.");

  std::stringstream buffer;
  // buffering value at chunk_offset
  auto decoder = _attribute_vector->create_base_decoder();
  const auto value_id = decoder->get(chunk_offset);
  Assert(value_id != _null_value_id, "This operation does not support NULL values.");

  const auto value = _dictionary->at(value_id);
  buffer << value;
  const auto length = buffer.str().length();
  // writing byte representation of length
  buffer.write(reinterpret_cast<const char*>(&length), sizeof(length));

  // appending the new string to the already present string
  row_string += buffer.str();
}

template <typename T>
void NewDictionaryColumn<T>::copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const {
  PerformanceWarning("NewDictionaryColumn<T>::copy_value_to_value_column is potentially very slow.");

  auto& output_column = static_cast<ValueColumn<T>&>(value_column);
  auto& values_out = output_column.values();

  auto decoder = _attribute_vector->create_base_decoder();
  const auto value_id = decoder->get(chunk_offset);

  if (output_column.is_nullable()) {
    output_column.null_values().push_back(value_id == _null_value_id);
    values_out.push_back(value_id == _null_value_id ? T{} : _dictionary->at(value_id));
  } else {
    DebugAssert(value_id != _null_value_id, "Target column needs to be nullable.");

    values_out.push_back(_dictionary->at(value_id));
  }
}

template <typename T>
std::shared_ptr<BaseColumn> NewDictionaryColumn<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_attribute_vector_ptr = _attribute_vector->copy_using_allocator(alloc);
  auto new_dictionary = pmr_vector<T>{*_dictionary, alloc};
  auto new_dictionary_ptr = std::allocate_shared<pmr_vector<T>>(alloc, std::move(new_dictionary));
  return std::allocate_shared<NewDictionaryColumn<T>>(alloc, new_dictionary_ptr, new_attribute_vector_ptr,
                                                      _null_value_id);
}

template <typename T>
ValueID NewDictionaryColumn<T>::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  const auto typed_value = type_cast<T>(value);

  auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
}

template <typename T>
ValueID NewDictionaryColumn<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  const auto typed_value = type_cast<T>(value);

  auto it = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
}

template <typename T>
size_t NewDictionaryColumn<T>::unique_values_count() const {
  return _dictionary->size();
}

template <typename T>
std::shared_ptr<const BaseNsVector> NewDictionaryColumn<T>::attribute_vector() const {
  return _attribute_vector;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(NewDictionaryColumn);

}  // namespace opossum
