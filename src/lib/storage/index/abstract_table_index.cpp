#include "abstract_table_index.hpp"

namespace hyrise {

BaseTableIndexIterator::BaseTableIndexIterator(std::shared_ptr<std::shared_lock<std::shared_mutex>> data_access_lock)
    : _data_access_lock(std::move(data_access_lock)) {}

BaseTableIndexIterator::reference BaseTableIndexIterator::operator*() const {
  Fail("cannot dereference on empty iterator");
}

BaseTableIndexIterator& BaseTableIndexIterator::operator++() {
  return *this;
}

bool BaseTableIndexIterator::operator==(const BaseTableIndexIterator& other) const {
  return true;
}

bool BaseTableIndexIterator::operator!=(const BaseTableIndexIterator& other) const {
  return false;
}

std::shared_ptr<BaseTableIndexIterator> BaseTableIndexIterator::clone() const {
  return std::make_shared<BaseTableIndexIterator>();
}

IteratorWrapper::IteratorWrapper(std::shared_ptr<BaseTableIndexIterator>&& table_index_iterator_ptr)
    : _impl(std::move(table_index_iterator_ptr)) {}

IteratorWrapper::IteratorWrapper(const IteratorWrapper& other) : _impl(other._impl->clone()) {}

IteratorWrapper& IteratorWrapper::operator=(const IteratorWrapper& other) {
  if (&other != this) {
    _impl = other._impl->clone();
  }

  return *this;
}

IteratorWrapper::reference IteratorWrapper::operator*() const {
  return _impl->operator*();
}

IteratorWrapper& IteratorWrapper::operator++() {
  _impl->operator++();
  return *this;
}

bool IteratorWrapper::operator==(const IteratorWrapper& other) const {
  return _impl->operator==(*other._impl);
}

bool IteratorWrapper::operator!=(const IteratorWrapper& other) const {
  return _impl->operator!=(*other._impl);
}

AbstractTableIndex::AbstractTableIndex(const TableIndexType type) : _type(type) {}

AbstractTableIndex::IteratorPair AbstractTableIndex::range_equals(const AllTypeVariant& value) const {
  return _range_equals(value);
}

std::pair<AbstractTableIndex::IteratorPair, AbstractTableIndex::IteratorPair> AbstractTableIndex::range_not_equals(
    const AllTypeVariant& value) const {
  return _range_not_equals(value);
}

AbstractTableIndex::Iterator AbstractTableIndex::cbegin() const {
  return _cbegin();
}

AbstractTableIndex::Iterator AbstractTableIndex::cend() const {
  return _cend();
}

AbstractTableIndex::Iterator AbstractTableIndex::null_cbegin() const {
  return _null_cbegin();
}

AbstractTableIndex::Iterator AbstractTableIndex::null_cend() const {
  return _null_cend();
}

template <typename Functor>
void AbstractTableIndex::access_values_with_iterators(const Functor& functor) const {
  functor(_cbegin(), _cend());
}

template <typename Functor>
void AbstractTableIndex::access_null_values_with_iterators(const Functor& functor) const {
  functor(_null_cbegin(), _null_cend());
}

template <typename Functor>
void AbstractTableIndex::range_equals_with_iterators(const Functor& functor, const AllTypeVariant& value) const {
  const auto [index_begin, index_end] = _range_equals(value);
  functor(index_begin, index_end);
}

template <typename Functor>
void AbstractTableIndex::range_not_equals_with_iterators(const Functor& functor, const AllTypeVariant& value) const {
  const auto [not_equals_range_left, not_equals_range_right] = _range_not_equals(value);
  functor(not_equals_range_left.first, not_equals_range_left.second);
  functor(not_equals_range_right.first, not_equals_range_right.second);
}

TableIndexType AbstractTableIndex::type() const {
  return _type;
}

size_t AbstractTableIndex::estimate_memory_usage() const {
  auto bytes = size_t{0u};
  bytes += sizeof(_type);
  bytes += _estimate_memory_usage();
  return bytes;
}

bool AbstractTableIndex::is_index_for(const ColumnID column_id) const {
  return _is_index_for(column_id);
}

std::unordered_set<ChunkID> AbstractTableIndex::get_indexed_chunk_ids() const {
  return _get_indexed_chunk_ids();
}

ColumnID AbstractTableIndex::get_indexed_column_id() const {
  return _get_indexed_column_id();
}

}  // namespace hyrise
