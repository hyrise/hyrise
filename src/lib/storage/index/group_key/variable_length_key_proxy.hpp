#pragma once

#include "variable_length_key_base.hpp"

namespace opossum {

class VariableLengthKey;

/**
 * Proxy for VariableLengthKey mimicing const VariableLengthKey &. The proxy is neccessary in order to directly read the
 * data hold by VariableLengthKeyStore.
 */
class VariableLengthKeyConstProxy {
  friend class VariableLengthKey;
  friend class VariableLengthKeyStore;
  friend class VariableLengthKeyProxy;

 public:
  VariableLengthKeyConstProxy() = default;

  VariableLengthKeyConstProxy(const VariableLengthKeyConstProxy &rhs) = default;
  VariableLengthKeyConstProxy &operator=(const VariableLengthKeyConstProxy &rhs) = delete;

  virtual ~VariableLengthKeyConstProxy() = default;

  /**
   * Implicitely convert proxy into VariableLengthKey in order to allow easy usage of VariableLengthKeyStore.
   */
  operator VariableLengthKey() const;  // NOLINT(runtime/explicit)

  CompositeKeyLength bytes_per_key() const;

  bool operator==(const VariableLengthKeyConstProxy &rhs) const;
  bool operator==(const VariableLengthKey &rhs) const;
  bool operator!=(const VariableLengthKeyConstProxy &rhs) const;
  bool operator!=(const VariableLengthKey &rhs) const;
  bool operator<(const VariableLengthKeyConstProxy &rhs) const;
  bool operator<(const VariableLengthKey &rhs) const;

  friend std::ostream &operator<<(std::ostream &os, const VariableLengthKeyConstProxy &key);

 protected:
  explicit VariableLengthKeyConstProxy(VariableLengthKeyWord *data, CompositeKeyLength bytes_per_key);

 protected:
  VariableLengthKeyBase _impl;
};

/**
 * Proxy for VariableLengthKey mimicing VariableLengthKey &. The proxy is neccessary in order to directly manipulate the
 * data held by the VariableLengthKeyStore.
 * Although both proxy classes are mostly used by-value, inheriting from VariableLengthKeyConstProxy is possible, since
 * no virtual functions are used and no members are provided, so that object slicing does not harm. Additionally,
 * inheritence allows the use of mutable proxy if const proxy is expected without further effort.
 */
class VariableLengthKeyProxy : public VariableLengthKeyConstProxy {
  friend class VariableLengthKey;
  friend class VariableLengthKeyStore;
  template <typename>
  friend class VariableLengthKeyStoreIteratorBase;

 public:
  VariableLengthKeyProxy() = default;

  VariableLengthKeyProxy(const VariableLengthKeyProxy &rhs) = default;
  VariableLengthKeyProxy &operator=(const VariableLengthKeyProxy &rhs);
  VariableLengthKeyProxy &operator=(const VariableLengthKeyConstProxy &rhs);

  VariableLengthKeyProxy &operator=(const VariableLengthKey &key);
  VariableLengthKeyProxy &operator<<=(CompositeKeyLength shift);
  VariableLengthKeyProxy &operator|=(uint64_t rhs);

  VariableLengthKeyProxy &shift_and_set(uint64_t value, uint8_t bits_to_set);

 private:
  explicit VariableLengthKeyProxy(VariableLengthKeyWord *data, CompositeKeyLength bytes_per_key);
  VariableLengthKeyProxy &operator=(const VariableLengthKeyBase &rhs);
};

}  // namespace opossum
