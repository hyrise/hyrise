#pragma once

#include <memory>
#include <ostream>

#include "../../../types.hpp"
#include "variable_length_key_base.hpp"

namespace opossum {

class VariableLengthKeyProxy;
class VariableLengthKeyConstProxy;

/**
 * The VariableLengthKey class can be used to create keys with a length up to 255 byte.
 * The keys are interpreted like unsigned integers, in contrast to big unsigned integer implementations no mathematical
 * operations are provided.
 * Each key manages its own memory, so the key itself points to the data in memory.
 * Since this class wraps VariableLengthKeyBase (and adds memory management) please refer to its documentation for most
 * member functions.
 */
class VariableLengthKey {
  friend class VariableLengthKeyProxy;
  friend class VariableLengthKeyConstProxy;

 public:
  VariableLengthKey() = default;
  explicit VariableLengthKey(CompositeKeyLength bytes_per_key);

  /**
   * Deep copies the data of rhs.
   */
  VariableLengthKey(const VariableLengthKey &rhs);
  VariableLengthKey &operator=(const VariableLengthKey &rhs);

  VariableLengthKey(VariableLengthKey &&rhs) = default;
  VariableLengthKey &operator=(VariableLengthKey &&rhs) = default;

  CompositeKeyLength bytes_per_key() const;

  bool operator==(const VariableLengthKey &rhs) const;
  bool operator==(const VariableLengthKeyConstProxy &rhs) const;
  bool operator!=(const VariableLengthKey &rhs) const;
  bool operator!=(const VariableLengthKeyConstProxy &rhs) const;
  bool operator<(const VariableLengthKey &rhs) const;
  bool operator<(const VariableLengthKeyConstProxy &rhs) const;

  VariableLengthKey &operator<<=(CompositeKeyLength shift);
  VariableLengthKey &operator|=(uint64_t rhs);

  VariableLengthKey &shift_and_set(uint64_t value, uint8_t bits_to_set);

  friend std::ostream &operator<<(std::ostream &os, const VariableLengthKey &key);

 private:
  explicit VariableLengthKey(const VariableLengthKeyBase &rhs);
  VariableLengthKey &operator=(const VariableLengthKeyBase &rhs);

 private:
  std::unique_ptr<VariableLengthKeyWord[]> _owned_data;
  VariableLengthKeyBase _impl;
};
}  // namespace opossum
