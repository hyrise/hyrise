#include "flattened_map_iterator.hpp"
#include "base_iterator.hpp"

namespace hyrise {

template <typename DataType>
FlattenedMapIterator<DataType>::FlattenedMapIterator(MapIterator it) : _map_iterator(it), _vector_index(0) {}

// template <typename DataType>
// FlattenedMapIterator<DataType>::FlattenedMapIterator(const FlattenedMapIterator<DataType>& other) {
//    _map_iterator = other._map_iterator;
//    _vector_index = other._vector_index;
//  }

template <typename DataType>
const RowID& FlattenedMapIterator<DataType>::operator*() const {
  return _map_iterator->second[_vector_index];
}

// template <typename DataType>
// FlattenedMapIterator<DataType>& FlattenedMapIterator<DataType>::operator=(const FlattenedMapIterator<DataType>& other) {
//   if (&other != this) {
//      _map_iterator = other._map_iterator;
//      _vector_index = other._vector_index;
//   }

//   return *this;
// }

template <typename DataType>
FlattenedMapIterator<DataType>& FlattenedMapIterator<DataType>::operator++() {
  if (++_vector_index >= _map_iterator->second.size()) {
    ++_map_iterator;
    _vector_index = 0;
  }
  return *this;
}

template <typename DataType>
bool FlattenedMapIterator<DataType>::operator==(const BaseIteratorImpl& other) const {
  auto other_iterator = dynamic_cast<const FlattenedMapIterator*>(&other);
  return other_iterator && _map_iterator == other_iterator->_map_iterator &&
         _vector_index == other_iterator->_vector_index;
}

template <typename DataType>
bool FlattenedMapIterator<DataType>::operator!=(const BaseIteratorImpl& other) const {
  auto other_iterator = dynamic_cast<const FlattenedMapIterator*>(&other);
  return !other_iterator || _map_iterator != other_iterator->_map_iterator ||
         _vector_index != other_iterator->_vector_index;
}

template <typename DataType>
std::shared_ptr<BaseIteratorImpl> FlattenedMapIterator<DataType>::clone() const {
  return std::make_shared<FlattenedMapIterator<DataType>>(*this);
}

template <typename DataType>
BaseIterator FlattenedMapIterator<DataType>::iterator_wrapper(MapIterator it) {
  return BaseIterator(std::make_shared<FlattenedMapIterator<DataType>>(it));
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(FlattenedMapIterator);

}  // namespace hyrise
