#pragma once

#include <cstdint>
#include <ostream>

namespace opossum {

/**
 * Internal type to save the provided data. Parts of the current implementation expecting
 * VariableLengthKeyWord to be a single byte. Known places are protected with static_assert.
 */
using VariableLengthKeyWord = uint8_t;

/**
 * The byte length of a composite key. Has to be an unsigned type.
 */
using CompositeKeyLength = uint8_t;

/**
 * This class implements all logic required for manipulating keys of variable length.
 * VariableLengthKey, VariableLengthKeyProxy and VariableLengthKeyConstProxy are wrappers
 * around this class, limiting the access to methods and managing the ownership of memory.
 */
class VariableLengthKeyBase {
 public:
  VariableLengthKeyBase() = default;
  VariableLengthKeyBase(VariableLengthKeyWord* data, CompositeKeyLength size);

  /**
   * Performs an 'or'-assignment on the eight (at most) least significant bytes. If more bits have to be set, shifting
   * has to
   * be performed.
   */
  VariableLengthKeyBase& operator|=(uint64_t other);

  /**
   * Shifts the data shift bits into the direction of the most significant bits. Empty positions are filled with zeros.
   */
  VariableLengthKeyBase& operator<<=(CompositeKeyLength shift);

  /**
   * Shifts the current key bits_to_set to the left and sets the least significant bits to the bits_to_set-least
   * significant bits of value.
   * The advantage to a combination of <<= and |= is that proper masking of the current key will be performed.
   */
  VariableLengthKeyBase& shift_and_set(uint64_t value, uint8_t bits_to_set);

 public:
  VariableLengthKeyWord* _data;
  CompositeKeyLength _size;
};

/**
 * Compare keys, interpreting the data in memory as unsigned integer numbers.
 */
bool operator==(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right);
bool operator!=(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right);
bool operator<(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right);
bool operator<=(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right);
bool operator>(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right);
bool operator>=(const VariableLengthKeyBase& left, const VariableLengthKeyBase& right);

/**
 * Prints the data as hex number.
 */
std::ostream& operator<<(std::ostream& os, const VariableLengthKeyBase& key);

}  // namespace opossum
