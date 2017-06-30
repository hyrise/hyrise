#include "variable_length_key_proxy.hpp"

#include <algorithm>
#include <exception>
#include <ostream>

#include "utils/assert.hpp"

namespace opossum {

// Const VariableLengthKeyProxy
VariableLengthKeyConstProxy::VariableLengthKeyConstProxy(VariableLengthKeyWord *data, CompositeKeyLength bytes_per_key)
    : _impl(data, bytes_per_key) {}

VariableLengthKeyConstProxy::operator VariableLengthKey() const { return VariableLengthKey(_impl); }

CompositeKeyLength VariableLengthKeyConstProxy::bytes_per_key() const { return _impl._size; }

bool VariableLengthKeyConstProxy::operator==(const VariableLengthKeyConstProxy &rhs) const {
  return _impl == rhs._impl;
}

bool VariableLengthKeyConstProxy::operator!=(const VariableLengthKeyConstProxy &rhs) const {
  return _impl != rhs._impl;
}

bool VariableLengthKeyConstProxy::operator<(const VariableLengthKeyConstProxy &rhs) const { return _impl < rhs._impl; }

bool VariableLengthKeyConstProxy::operator==(const VariableLengthKey &rhs) const { return _impl == rhs._impl; }
bool VariableLengthKeyConstProxy::operator!=(const VariableLengthKey &rhs) const { return _impl != rhs._impl; }
bool VariableLengthKeyConstProxy::operator<(const VariableLengthKey &rhs) const { return _impl < rhs._impl; }

std::ostream &operator<<(std::ostream &os, const VariableLengthKeyConstProxy &key) {
  os << key._impl;
  return os;
}

// Mutable VariableLengthKeyProxy
VariableLengthKeyProxy::VariableLengthKeyProxy(VariableLengthKeyWord *data, CompositeKeyLength bytes_per_key)
    : VariableLengthKeyConstProxy(data, bytes_per_key) {}

VariableLengthKeyProxy &VariableLengthKeyProxy::operator=(const VariableLengthKeyBase &rhs) {
  DebugAssert(
      (_impl._size == rhs._size),
      "Copying the data of a VariableLengthKey to a VariableLenghtKeyProxy requires that both have the same key size.");

  std::copy(rhs._data, rhs._data + _impl._size, _impl._data);
  return *this;
}

VariableLengthKeyProxy &VariableLengthKeyProxy::operator=(const VariableLengthKey &rhs) { return operator=(rhs._impl); }
VariableLengthKeyProxy &VariableLengthKeyProxy::operator=(const VariableLengthKeyProxy &rhs) {
  return operator=(rhs._impl);
}

VariableLengthKeyProxy &VariableLengthKeyProxy::operator<<=(CompositeKeyLength shift) {
  _impl <<= shift;
  return *this;
}

VariableLengthKeyProxy &VariableLengthKeyProxy::operator|=(uint64_t rhs) {
  _impl |= rhs;
  return *this;
}

VariableLengthKeyProxy &VariableLengthKeyProxy::shift_and_set(uint64_t value, uint8_t bits_to_set) {
  _impl.shift_and_set(value, bits_to_set);
  return *this;
}

}  // namespace opossum
