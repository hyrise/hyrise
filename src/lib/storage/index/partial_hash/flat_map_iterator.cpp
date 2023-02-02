#include "flat_map_iterator.hpp"

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
FlatMapIterator FlatMapIterator::create_iterator(const tsl::sparse_map<DataType, std::vector<RowID>>::const_iterator& it) {
   return FlatMapIterator(std::make_unique<FlatMapIteratorImpl<DataType>>(it));
}

template FlatMapIterator FlatMapIterator::create_iterator<int32_t>(const tsl::sparse_map<int, std::vector<RowID>>::const_iterator& it);
template FlatMapIterator FlatMapIterator::create_iterator<int64_t>(const tsl::sparse_map<long, std::vector<RowID>>::const_iterator& it);
template FlatMapIterator FlatMapIterator::create_iterator<float>(const tsl::sparse_map<float, std::vector<RowID>>::const_iterator& it);
template FlatMapIterator FlatMapIterator::create_iterator<double>(const tsl::sparse_map<double, std::vector<RowID>>::const_iterator& it);
template FlatMapIterator FlatMapIterator::create_iterator<pmr_string>(const tsl::sparse_map<pmr_string, std::vector<RowID>>::const_iterator& it);

}  // namespace hyrise
