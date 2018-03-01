#include "value_vector.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
ValueVector<T>::ValueVector() {}

template <typename T>
void ValueVector<T>::push_back(const T& value) {
  _values.push_back(std::forward<const T>(value));
}

template <typename T>
void ValueVector<T>::push_back(T&& value) {
  _values.push_back(std::forward<T>(value));
}

template <typename T>
T& ValueVector<T>::at(const ChunkOffset chunk_offset) {
  return _values.at(chunk_offset);
}

template <typename T>
typename ValueVector<T>::iterator ValueVector<T>::begin() noexcept {
  return _values.begin();
}

template <typename T>
typename ValueVector<T>::iterator ValueVector<T>::end() noexcept {
  return _values.end();
}

template <typename T>
typename ValueVector<T>::const_iterator ValueVector<T>::begin() const noexcept {
  return _values.begin();
}

template <typename T>
typename ValueVector<T>::const_iterator ValueVector<T>::end() const noexcept {
  return _values.end();
}

template <typename T>
typename ValueVector<T>::reverse_iterator ValueVector<T>::rbegin() noexcept {
  return _values.rbegin();
}

template <typename T>
typename ValueVector<T>::reverse_iterator ValueVector<T>::rend() noexcept {
  return _values.rend();
}

template <typename T>
typename ValueVector<T>::const_iterator ValueVector<T>::cbegin() const noexcept {
  return _values.cbegin();
}

template <typename T>
typename ValueVector<T>::const_iterator ValueVector<T>::cend() const noexcept {
  return _values.cend();
}

template <typename T>
void ValueVector<T>::erase(iterator start, iterator end) {
  _values.erase(start, end);
}

template <typename T>
T& ValueVector<T>::operator[](const size_t n) {
  PerformanceWarning("operator[] used");
  return _values[n];
}

template <typename T>
const T& ValueVector<T>::operator[](const size_t n) const {
  PerformanceWarning("operator[] used");
  return _values[n];
}

template <typename T>
size_t ValueVector<T>::size() const {
  return _values.size();
}

template <typename T>
size_t ValueVector<T>::capacity() const {
  return _values.capacity();
}

template <typename T>
void ValueVector<T>::shrink_to_fit() {
  _values.shrink_to_fit();
}

template <typename T>
PolymorphicAllocator<T> ValueVector<T>::get_allocator() {
  return _values.get_allocator();
}

template <typename T>
void ValueVector<T>::reserve(const size_t n) {
  _values.reserve(n);
}

template <typename T>
const pmr_vector<T>& ValueVector<T>::values() const {
  return _values;
}

template <typename T>
const T* ValueVector<T>::data() const {
  return &_values[0];
}

template <typename T>
size_t ValueVector<T>::data_size() const {
  return sizeof(*this) + _values.size() * sizeof(typename decltype(_values)::value_type);
}

// Implementation of ValueVector<FixedString> starts here

void ValueVector<FixedString>::push_back(const std::string& string) {
  const auto pos = _chars.size();
  _chars.resize(_chars.size() + _string_length);
  string.copy(&_chars[pos], _string_length);
  if (string.size() < _string_length) {
    std::fill(_chars.begin() + pos + string.size(), _chars.begin() + pos + _string_length, '\0');
  }
}

FixedString ValueVector<FixedString>::at(const ChunkOffset chunk_offset) {
  return FixedString(&_chars.at(chunk_offset * _string_length), _string_length);
}

ValueVector<FixedString>::iterator ValueVector<FixedString>::begin() noexcept {
  return iterator(_string_length, _chars, 0);
}

ValueVector<FixedString>::iterator ValueVector<FixedString>::end() noexcept {
  return iterator(_string_length, _chars, _chars.size());
}

ValueVector<FixedString>::iterator ValueVector<FixedString>::begin() const noexcept {
  return iterator(_string_length, _chars, 0);
}

ValueVector<FixedString>::iterator ValueVector<FixedString>::end() const noexcept {
  return iterator(_string_length, _chars, _chars.size());
}

ValueVector<FixedString>::iterator ValueVector<FixedString>::cbegin() const noexcept {
  return iterator(_string_length, _chars, 0);
}

ValueVector<FixedString>::iterator ValueVector<FixedString>::cend() const noexcept {
  return iterator(_string_length, _chars, _chars.size());
}

typedef boost::reverse_iterator<ValueVector<FixedString>::iterator> reverse_iterator;
reverse_iterator ValueVector<FixedString>::rbegin() noexcept { return reverse_iterator(end()); }

reverse_iterator ValueVector<FixedString>::rend() noexcept { return reverse_iterator(begin()); }

const std::string ValueVector<FixedString>::operator[](const size_t n) const {
  PerformanceWarning("operator[] used");
  const auto string_value = std::string(&_chars[n * _string_length], _string_length);
  const auto pos = string_value.find('\0');

  if (pos == std::string::npos) {
    return string_value;
  } else {
    return string_value.substr(0, pos);
  }
}

size_t ValueVector<FixedString>::size() const { return _chars.size() / _string_length; }

size_t ValueVector<FixedString>::capacity() const { return _chars.capacity(); }

void ValueVector<FixedString>::erase(const iterator start, const iterator end) {
  auto it = _chars.begin();
  std::advance(it, _chars.size() - std::distance(start, end) * _string_length);
  _chars.erase(it, _chars.end());
}

void ValueVector<FixedString>::shrink_to_fit() { _chars.shrink_to_fit(); }

PolymorphicAllocator<FixedString> ValueVector<FixedString>::get_allocator() { return _chars.get_allocator(); }

void ValueVector<FixedString>::reserve(const size_t n) { _chars.reserve(n * _string_length); }

const char* ValueVector<FixedString>::data() const { return &_chars[0]; }

size_t ValueVector<FixedString>::data_size() const { return sizeof(*this) + _chars.size(); }

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueVector);

}  // namespace opossum
