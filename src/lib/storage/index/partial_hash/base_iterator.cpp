#include "base_iterator.hpp"

namespace hyrise {

BaseIterator::BaseIterator(std::shared_ptr<BaseIteratorImpl>&& index_iterator) : _impl(std::move(index_iterator)) {}

BaseIterator::BaseIterator(const BaseIterator& other) : _impl(other._impl->clone()) {}

BaseIterator& BaseIterator::operator=(const BaseIterator& other) {
  if (&other != this) {
    _impl = other._impl->clone();
  }

  return *this;
}

BaseIterator::reference BaseIterator::operator*() const {
  return _impl->operator*();
}

BaseIterator& BaseIterator::operator++() {
  _impl->operator++();
  return *this;
}

bool BaseIterator::operator==(const BaseIterator& other) const {
  return _impl->operator==(*other._impl);
}

bool BaseIterator::operator!=(const BaseIterator& other) const {
  return _impl->operator!=(*other._impl);
}

}  // namespace hyrise
