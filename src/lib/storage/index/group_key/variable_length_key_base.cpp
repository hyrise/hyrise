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
std::pair<uintX_t, uintX_t> shift_left_with_borrow(uintX_t value, hyrise::CompositeKeyLength bits) {
  const auto bits_for_type = sizeof(uintX_t) * CHAR_BIT;
  assert(bits <= bits_for_type);
  auto borrow = value;
  borrow >>= bits_for_type - bits;
  auto shifted_value = value << bits;
  return {shifted_value, borrow};
}
}  // namespace

namespace hyrise {

VariableLengthKeyBase::VariableLengthKeyBase(VariableLengthKeyWord* data, CompositeKeyLength size)
    : _data(data), _size(size) {}

VariableLengthKeyBase& VariableLengthKeyBase::operator|=(uint64_t other) {
  static_assert(std::is_same_v<VariableLengthKeyWord, uint8_t>, "Changes for new word type required.");
  const auto* const raw_other = reinterpret_cast<VariableLengthKeyWord*>(&other);
  auto operation_width = std::min(static_cast<CompositeKeyLength>(sizeof(other)), _size);
  for (auto index = CompositeKeyLength{0}; index < operation_width; ++index) {
    if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
      _data[index] |= raw_other[index];
    } else {
      _data[_size - 1 - index] |= raw_other[8 - 1 - index];
    }
  }
  return *this;
}

VariableLengthKeyBase& VariableLengthKeyBase::operator<<=(CompositeKeyLength shift) {
  static_assert(std::is_same_v<VariableLengthKeyWord, uint8_t>, "Changes for new word type required.");
  const auto byte_shift = shift / CHAR_BIT;
  const auto bit_shift = static_cast<CompositeKeyLength>(shift % CHAR_BIT);
  if (byte_shift >= _size) {
    std::fill(_data, _data + _size, static_cast<VariableLengthKeyWord>(0u));
  } else {
    if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
      // perform shifting
      for (auto index = _size - 1; index > byte_shift - 1; --index) {
        const auto [value, borrow] = shift_left_with_borrow(_data[index - byte_shift], bit_shift);
        _data[index] = value;
        if (index + 1 < _size) {
          _data[index + 1] |= borrow;
        }
      }

      // fill now "empty" positions with zeros
      std::fill(_data, _data + byte_shift, VariableLengthKeyWord{0});
    } else {
      // perform shifting
      for (auto index = int16_t{0}; index < _size - static_cast<int16_t>(byte_shift); ++index) {
        const auto [value, borrow] = shift_left_with_borrow(_data[index + byte_shift], bit_shift);
        _data[index] = value;
        if (index > 0) {
          _data[index - 1] |= borrow;
        }
      }

      // fill now "empty" positions with zeros
      std::fill(_data + _size - byte_shift, _data + _size, VariableLengthKeyWord{0});
    }
  }
  return *this;
}

VariableLengthKeyBase& VariableLengthKeyBase::shift_and_set(uint64_t value, uint8_t bits_to_set) {
  auto mask = uint64_t{0xFFFFFFFFFFFFFFFF};
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

bool operator!=(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) {
  return !(left == right);
}

bool operator<(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) {
  static_assert(std::is_same_v<VariableLengthKeyWord, uint8_t>, "Changes for new word type required.");
  if (left._size != right._size) {
    return left._size < right._size;
  }

  if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
    // compare right to left since most significant byte is on the right
    // memcmp can not be used since it performs lexical comparison
    // loop overflows after iteration with i == 0, so i becomes greater than left._size
    for (CompositeKeyLength index = left._size - 1; index < left._size; --index) {
      if (left._data[index] == right._data[index]) {
        continue;
      }
      return left._data[index] < right._data[index];
    }
  } else {
    return std::memcmp(left._data, right._data, left._size) < 0;
  }

  return false;
}

bool operator<=(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) {
  return left < right || left == right;
}

bool operator>(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) {
  return !(left <= right);
}

bool operator>=(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) {
  return !(left < right);
}

std::ostream& operator<<(std::ostream& stream, const VariableLengthKeyBase& key) {
  stream << std::hex << std::setfill('0');
  const auto* const raw_data = reinterpret_cast<uint8_t*>(key._data);

  if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
    for (auto key_id = CompositeKeyLength{1}; key_id <= key._size; ++key_id) {
      stream << std::setw(2) << +raw_data[key._size - key_id];
      if (key_id != key._size) {
        stream << ' ';
      }
    }
  } else {
    for (auto key_id = CompositeKeyLength{0}; key_id < key._size; ++key_id) {
      stream << std::setw(2) << +raw_data[key_id];
      if (key_id != key._size - 1) {
        stream << ' ';
      }
    }
  }

  stream << std::dec << std::setw(0) << std::setfill(' ');
  return stream;
}
}  // namespace hyrise
