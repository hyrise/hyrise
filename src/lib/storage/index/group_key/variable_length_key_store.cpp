#include "variable_length_key_store.hpp"

#include <iterator>
#include <vector>

#include "variable_length_key_proxy.hpp"

namespace opossum {

VariableLengthKeyStore::VariableLengthKeyStore(ChunkOffset size, CompositeKeyLength bytes_per_key) {
  static const CompositeKeyLength alignment = 8u;
  _bytes_per_key = bytes_per_key;
  _key_alignment = (bytes_per_key / alignment + (bytes_per_key % alignment == 0u ? 0u : 1u)) * alignment;
  _data = std::vector<VariableLengthKeyWord>(size * _key_alignment);
}

VariableLengthKeyProxy VariableLengthKeyStore::operator[](ChunkOffset position) {
  return VariableLengthKeyProxy(_data.data() + static_cast<size_t>(position) * _key_alignment, _bytes_per_key);
}

VariableLengthKeyConstProxy VariableLengthKeyStore::operator[](ChunkOffset position) const {
  auto self = const_cast<VariableLengthKeyStore*>(this);  // NOLINT(cppcoreguidelines-pro-type-const-cast)
  // The const_cast is grandfathered in, because it would require significant changes to get rid of it. New code should
  // not require it.
  return VariableLengthKeyConstProxy(self->_data.data() + static_cast<size_t>(position) * _key_alignment,
                                     _bytes_per_key);
}

void VariableLengthKeyStore::resize(ChunkOffset size) { _data.resize(size * _key_alignment); }

void VariableLengthKeyStore::shrink_to_fit() { _data.shrink_to_fit(); }

CompositeKeyLength VariableLengthKeyStore::key_size() const { return _bytes_per_key; }

ChunkOffset VariableLengthKeyStore::size() const { return static_cast<ChunkOffset>(_data.size() / _key_alignment); }

VariableLengthKeyStore::iterator VariableLengthKeyStore::erase(iterator first, iterator last) {
  auto underlying_first = _data.begin();
  std::advance(underlying_first, std::distance(begin(), first) * _key_alignment);
  auto underlying_last = _data.begin();
  std::advance(underlying_last, std::distance(begin(), last) * _key_alignment);
  _data.erase(underlying_first, underlying_last);
  return end();
}

VariableLengthKeyStore::iterator VariableLengthKeyStore::begin() {
  return iterator(_bytes_per_key, _key_alignment, _data.data());
}

VariableLengthKeyStore::iterator VariableLengthKeyStore::end() {
  return iterator(_bytes_per_key, _key_alignment, _data.data() + _data.size());
}

VariableLengthKeyStore::const_iterator VariableLengthKeyStore::begin() const { return cbegin(); }

VariableLengthKeyStore::const_iterator VariableLengthKeyStore::end() const { return cend(); }

VariableLengthKeyStore::const_iterator VariableLengthKeyStore::cbegin() const {
  auto self = const_cast<VariableLengthKeyStore*>(this);  // NOLINT(cppcoreguidelines-pro-type-const-cast) (see above)
  return const_iterator(_bytes_per_key, _key_alignment, self->_data.data());
}

VariableLengthKeyStore::const_iterator VariableLengthKeyStore::cend() const {
  auto self = const_cast<VariableLengthKeyStore*>(this);  // NOLINT(cppcoreguidelines-pro-type-const-cast) (see above)
  return const_iterator(_bytes_per_key, _key_alignment, self->_data.data() + _data.size());
}

}  // namespace opossum
