#include "variable_length_key_base.hpp"

#include <algorithm>
#include <cassert>
#include <climits>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <ostream>
#include <type_traits>
#include <utility>

namespace {
template <typename uintX_t>  // NOLINT (We like uintX_t)
std::pair<uintX_t, uintX_t> shift_left_with_borrow(uintX_t value, opossum::CompositeKeyLength bits) {
  const auto bits_for_type = sizeof(uintX_t) * CHAR_BIT;
  assert(bits <= bits_for_type);
  auto borrow = value;
  borrow >>= bits_for_type - bits;
  auto shifted_value = value << bits;
  return {shifted_value, borrow};
}
}  // namespace

namespace opossum {

VariableLengthKeyBase::VariableLengthKeyBase(VariableLengthKeyWord* data, CompositeKeyLength size)
    : _data(data), _size(size) {}

VariableLengthKeyBase& VariableLengthKeyBase::operator|=(uint64_t other) {
  static_assert(std::is_same_v<VariableLengthKeyWord, uint8_t>, "Changes for new word type required.");
  auto raw_other = reinterpret_cast<VariableLengthKeyWord*>(&other);
  auto operation_width = std::min(static_cast<CompositeKeyLength>(sizeof(other)), _size);
  for (CompositeKeyLength i = 0; i < operation_width; ++i) {
    if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
      _data[i] |= raw_other[i];
    } else {
      _data[_size - 1 - i] |= raw_other[8 - 1 - i];
    }
  }
  return *this;
}

VariableLengthKeyBase& VariableLengthKeyBase::operator<<=(CompositeKeyLength shift) {
  static_assert(std::is_same_v<VariableLengthKeyWord, uint8_t>, "Changes for new word type required.");
  const auto byte_shift = shift / CHAR_BIT;
  const auto bit_shift = shift % CHAR_BIT;
  if (byte_shift >= _size) {
    std::fill(_data, _data + _size, static_cast<VariableLengthKeyWord>(0u));
  } else {
    if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
      // perform shifting
      for (int16_t i = _size - 1; i > static_cast<int16_t>(byte_shift) - 1; --i) {
        VariableLengthKeyWord value, borrow;
        std::tie(value, borrow) = shift_left_with_borrow(_data[i - byte_shift], bit_shift);
        _data[i] = value;
        if (i + 1 < _size) _data[i + 1] |= borrow;
      }

      // fill now "empty" positions with zeros
      std::fill(_data, _data + byte_shift, static_cast<VariableLengthKeyWord>(0u));
    } else {
      // perform shifting
      for (int16_t i = 0; i < _size - static_cast<int16_t>(byte_shift); ++i) {
        VariableLengthKeyWord value, borrow;
        std::tie(value, borrow) = shift_left_with_borrow(_data[i + byte_shift], bit_shift);
        _data[i] = value;
        if (i > 0) _data[i - 1] |= borrow;
      }

      // fill now "empty" positions with zeros
      std::fill(_data + _size - byte_shift, _data + _size, static_cast<VariableLengthKeyWord>(0u));
    }
  }
  return *this;
}

VariableLengthKeyBase& VariableLengthKeyBase::shift_and_set(uint64_t value, uint8_t bits_to_set) {
  uint64_t mask = 0xFFFFFFFFFFFFFFFF;
  // shifting is undefined if right operand is greater than or equal to the number of bits of left operand
  if (bits_to_set < sizeof(uint64_t) * CHAR_BIT) {
    mask = ~(mask << bits_to_set);
  }
  auto masked_value = value & mask;
  *this <<= bits_to_set;
  *this |= masked_value;
  return *this;
}

bool operator==(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) {
  return left._size == right._size && std::memcmp(left._data, right._data, left._size) == 0;
}

bool operator!=(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) { return !(left == right); }

bool operator<(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) {
  static_assert(std::is_same_v<VariableLengthKeyWord, uint8_t>, "Changes for new word type required.");
  if (left._size != right._size) return left._size < right._size;

  if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
    // compare right to left since most significant byte is on the right
    // memcmp can not be used since it performs lexical comparison
    // loop overflows after iteration with i == 0, so i becomes greater than left._size
    for (CompositeKeyLength i = left._size - 1; i < left._size; --i) {
      if (left._data[i] == right._data[i]) continue;
      return left._data[i] < right._data[i];
    }
  } else {
    return std::memcmp(left._data, right._data, left._size) < 0;
  }

  return false;
}

bool operator<=(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) {
  return left < right || left == right;
}

bool operator>(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) { return !(left <= right); }

bool operator>=(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) { return !(left < right); }

std::ostream& operator<<(std::ostream& os, const VariableLengthKeyBase& key) {
  os << std::hex << std::setfill('0');
  auto raw_data = reinterpret_cast<uint8_t*>(key._data);
  if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
    for (CompositeKeyLength i = 1; i <= key._size; ++i) {
      os << std::setw(2) << +raw_data[key._size - i];
      if (i != key._size) os << ' ';
    }
  } else {
    for (CompositeKeyLength i = 0; i < key._size; ++i) {
      os << std::setw(2) << +raw_data[i];
      if (i != key._size - 1) os << ' ';
    }
  }
  os << std::dec << std::setw(0) << std::setfill(' ');
  return os;
}
}  // namespace opossum
