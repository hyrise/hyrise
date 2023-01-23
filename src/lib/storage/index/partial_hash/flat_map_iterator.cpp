#include "flat_map_iterator.hpp"

namespace hyrise {

FlatMapIterator::FlatMapIterator(std::shared_ptr<BaseFlatMapIteratorImpl>&& index_iterator) : _impl(std::move(index_iterator)) {}

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

}  // namespace hyrise
