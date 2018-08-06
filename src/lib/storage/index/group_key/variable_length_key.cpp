#include "variable_length_key.hpp"

#include <algorithm>
#include <memory>
#include <ostream>
#include <utility>

#include "variable_length_key_proxy.hpp"
#include "variable_length_key_store.hpp"

namespace opossum {

VariableLengthKey::VariableLengthKey(CompositeKeyLength bytes_per_key)
    : _owned_data(std::make_unique<VariableLengthKeyWord[]>(bytes_per_key)), _impl(_owned_data.get(), bytes_per_key) {}

VariableLengthKey::VariableLengthKey(const VariableLengthKeyBase& other)
    : _owned_data(std::make_unique<VariableLengthKeyWord[]>(other._size)), _impl(_owned_data.get(), other._size) {
  std::copy(other._data, other._data + other._size, _impl._data);
}

VariableLengthKey::VariableLengthKey(const VariableLengthKey& other) : VariableLengthKey(other._impl) {}

VariableLengthKey& VariableLengthKey::operator=(const VariableLengthKeyBase& other) {
  auto temp = VariableLengthKey(other);
  std::swap(*this, temp);
  return *this;
}

VariableLengthKey& VariableLengthKey::operator=(const VariableLengthKey& other) {
  operator=(other._impl);
  return *this;
}

bool VariableLengthKey::operator==(const VariableLengthKey& other) const { return _impl == other._impl; }
bool VariableLengthKey::operator==(const VariableLengthKeyConstProxy& other) const { return _impl == other._impl; }

bool VariableLengthKey::operator!=(const VariableLengthKey& other) const { return _impl != other._impl; }
bool VariableLengthKey::operator!=(const VariableLengthKeyConstProxy& other) const { return _impl != other._impl; }

bool VariableLengthKey::operator<(const VariableLengthKey& other) const { return _impl < other._impl; }
bool VariableLengthKey::operator<(const VariableLengthKeyConstProxy& other) const { return _impl < other._impl; }

VariableLengthKey& VariableLengthKey::operator<<=(CompositeKeyLength shift) {
  _impl <<= shift;
  return *this;
}

VariableLengthKey& VariableLengthKey::operator|=(uint64_t other) {
  _impl |= other;
  return *this;
}

VariableLengthKey& VariableLengthKey::shift_and_set(uint64_t value, uint8_t bits_to_set) {
  _impl.shift_and_set(value, bits_to_set);
  return *this;
}

CompositeKeyLength VariableLengthKey::bytes_per_key() const { return _impl._size; }

std::ostream& operator<<(std::ostream& os, const VariableLengthKey& key) {
  os << key._impl;
  return os;
}
}  // namespace opossum
