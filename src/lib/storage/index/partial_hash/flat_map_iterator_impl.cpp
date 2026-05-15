#include "flat_map_iterator_impl.hpp"

#include <memory>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise {

template <typename DataType>
FlatMapIteratorImpl<DataType>::FlatMapIteratorImpl(MapIterator it) : _map_iterator(it), _vector_index(0) {}

template <typename DataType>
const RowID& FlatMapIteratorImpl<DataType>::operator*() const {
  return _map_iterator->second[_vector_index];
}

template <typename DataType>
FlatMapIteratorImpl<DataType>& FlatMapIteratorImpl<DataType>::operator++() {
  if (++_vector_index >= _map_iterator->second.size()) {
    ++_map_iterator;
    _vector_index = 0;
  }
  return *this;
}

template <typename DataType>
bool FlatMapIteratorImpl<DataType>::operator==(const BaseFlatMapIteratorImpl& other) const {
  auto other_iterator = static_cast<const FlatMapIteratorImpl*>(&other);
  return other_iterator && _map_iterator == other_iterator->_map_iterator &&
         _vector_index == other_iterator->_vector_index;
}

template <typename DataType>
bool FlatMapIteratorImpl<DataType>::operator!=(const BaseFlatMapIteratorImpl& other) const {
  auto other_iterator = static_cast<const FlatMapIteratorImpl*>(&other);
  return !other_iterator || _map_iterator != other_iterator->_map_iterator ||
         _vector_index != other_iterator->_vector_index;
}

template <typename DataType>
std::unique_ptr<BaseFlatMapIteratorImpl> FlatMapIteratorImpl<DataType>::clone() const {
  return std::make_unique<FlatMapIteratorImpl<DataType>>(*this);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(FlatMapIteratorImpl);

}  // namespace hyrise
