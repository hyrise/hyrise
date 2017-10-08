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

VariableLengthKey::VariableLengthKey(const VariableLengthKeyBase& rhs)
    : _owned_data(std::make_unique<VariableLengthKeyWord[]>(rhs._size)), _impl(_owned_data.get(), rhs._size) {
  std::copy(rhs._data, rhs._data + rhs._size, _impl._data);
}

VariableLengthKey::VariableLengthKey(const VariableLengthKey& other) : VariableLengthKey(other._impl) {}

VariableLengthKey& VariableLengthKey::operator=(const VariableLengthKeyBase& rhs) {
  auto temp = VariableLengthKey(rhs);
  std::swap(*this, temp);
  return *this;
}

VariableLengthKey& VariableLengthKey::operator=(const VariableLengthKey& rhs) { return operator=(rhs._impl); }

bool VariableLengthKey::operator==(const VariableLengthKey& rhs) const { return _impl == rhs._impl; }
bool VariableLengthKey::operator==(const VariableLengthKeyConstProxy& rhs) const { return _impl == rhs._impl; }

bool VariableLengthKey::operator!=(const VariableLengthKey& rhs) const { return _impl != rhs._impl; }
bool VariableLengthKey::operator!=(const VariableLengthKeyConstProxy& rhs) const { return _impl != rhs._impl; }

bool VariableLengthKey::operator<(const VariableLengthKey& rhs) const { return _impl < rhs._impl; }
bool VariableLengthKey::operator<(const VariableLengthKeyConstProxy& rhs) const { return _impl < rhs._impl; }

VariableLengthKey& VariableLengthKey::operator<<=(CompositeKeyLength shift) {
  _impl <<= shift;
  return *this;
}

VariableLengthKey& VariableLengthKey::operator|=(uint64_t rhs) {
  _impl |= rhs;
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
