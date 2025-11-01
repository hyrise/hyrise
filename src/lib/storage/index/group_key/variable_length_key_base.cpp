#include "variable_length_key_base.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <climits>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <ios>
#include <ostream>
#include <type_traits>
#include <utility>

namespace {
template <typename uintX_t>  // NOLINT(readability-identifier-naming): UintXT looks awkward.
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

VariableLengthKeyBase::VariableLengthKeyBase(VariableLengthKeyWord* init_data, CompositeKeyLength init_size)
    : data(init_data), size(init_size) {}

VariableLengthKeyBase& VariableLengthKeyBase::operator|=(uint64_t other) {
  static_assert(std::is_same_v<VariableLengthKeyWord, uint8_t>, "Changes for new word type required.");
  const auto* const raw_other = reinterpret_cast<VariableLengthKeyWord*>(&other);
  auto operation_width = std::min(static_cast<CompositeKeyLength>(sizeof(other)), size);
  for (auto index = CompositeKeyLength{0}; index < operation_width; ++index) {
    if constexpr (std::endian::native == std::endian::little) {
      data[index] |= raw_other[index];
    } else {
      data[size - 1 - index] |= raw_other[8 - 1 - index];
    }
  }
  return *this;
}

VariableLengthKeyBase& VariableLengthKeyBase::operator<<=(CompositeKeyLength shift) {
  static_assert(std::is_same_v<VariableLengthKeyWord, uint8_t>, "Changes for new word type required.");
  const auto byte_shift = shift / CHAR_BIT;
  const auto bit_shift = static_cast<CompositeKeyLength>(shift % CHAR_BIT);
  if (static_cast<CompositeKeyLength>(byte_shift) >= size) {
    std::fill(data, data + size, static_cast<VariableLengthKeyWord>(0));
  } else {
    if constexpr (std::endian::native == std::endian::little) {
      // perform shifting
      for (auto index = size - 1; index > byte_shift - 1; --index) {
        const auto [value, borrow] = shift_left_with_borrow(data[index - byte_shift], bit_shift);
        data[index] = value;
        if (index + 1 < size) {
          data[index + 1] |= borrow;
        }
      }

      // fill now "empty" positions with zeros
      std::fill(data, data + byte_shift, VariableLengthKeyWord{0});
    } else {
      // perform shifting
      for (auto index = int16_t{0}; index < size - static_cast<int16_t>(byte_shift); ++index) {
        const auto [value, borrow] = shift_left_with_borrow(data[index + byte_shift], bit_shift);
        data[index] = value;
        if (index > 0) {
          data[index - 1] |= borrow;
        }
      }

      // fill now "empty" positions with zeros
      std::fill(data + size - byte_shift, data + size, VariableLengthKeyWord{0});
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
  return left.size == right.size && std::memcmp(left.data, right.data, left.size) == 0;
}

bool operator!=(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) {
  return !(left == right);
}

bool operator<(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right) {
  static_assert(std::is_same_v<VariableLengthKeyWord, uint8_t>, "Changes for new word type required.");
  if (left.size != right.size) {
    return left.size < right.size;
  }

  if constexpr (std::endian::native == std::endian::little) {
    // compare right to left since most significant byte is on the right
    // memcmp can not be used since it performs lexical comparison
    // loop overflows after iteration with i == 0, so i becomes greater than left.size
    for (CompositeKeyLength index = left.size - 1; index < left.size; --index) {
      if (left.data[index] == right.data[index]) {
        continue;
      }
      return left.data[index] < right.data[index];
    }
  } else {
    return std::memcmp(left.data, right.data, left.size) < 0;
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
  const auto* const rawdata = reinterpret_cast<uint8_t*>(key.data);

  if constexpr (std::endian::native == std::endian::little) {
    for (auto key_id = CompositeKeyLength{1}; key_id <= key.size; ++key_id) {
      stream << std::setw(2) << +rawdata[key.size - key_id];
      if (key_id != key.size) {
        stream << ' ';
      }
    }
  } else {
    for (auto key_id = CompositeKeyLength{0}; key_id < key.size; ++key_id) {
      stream << std::setw(2) << +rawdata[key_id];
      if (key_id != key.size - 1) {
        stream << ' ';
      }
    }
  }

  stream << std::dec << std::setw(0) << std::setfill(' ');
  return stream;
}
}  // namespace hyrise
