#include "flat_map_iterator.hpp"

#include <tsl/sparse_map.h>

#include <memory>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "flat_map_iterator_impl.hpp"

namespace hyrise {

FlatMapIterator::FlatMapIterator(std::unique_ptr<BaseFlatMapIteratorImpl>&& index_iterator)
    : _impl(std::move(index_iterator)) {}

FlatMapIterator::FlatMapIterator(const FlatMapIterator& other) : _impl(other._impl->clone()) {}

FlatMapIterator& FlatMapIterator::operator=(const FlatMapIterator& other) {
  if (&other != this) {
    _impl = other._impl->clone();
  }

  return *this;
}

FlatMapIterator::reference FlatMapIterator::operator*() const {
  return _impl->operator*();
}

FlatMapIterator& FlatMapIterator::operator++() {
  _impl->operator++();
  return *this;
}

bool FlatMapIterator::operator==(const FlatMapIterator& other) const {
  return _impl->operator==(*other._impl);
}

bool FlatMapIterator::operator!=(const FlatMapIterator& other) const {
  return _impl->operator!=(*other._impl);
}

template <typename DataType>
FlatMapIterator CreateFlatMapIterator<DataType>::from_map_iterator(
    const typename tsl::sparse_map<DataType, std::vector<RowID>>::const_iterator& it) {
  return FlatMapIterator(std::make_unique<FlatMapIteratorImpl<DataType>>(it));
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(CreateFlatMapIterator);

}  // namespace hyrise
