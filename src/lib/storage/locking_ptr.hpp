#pragma once

#include <shared_mutex>

namespace opossum {

/**
 * Smart pointer that locks a given mutex on construction and
 * unlocks it on desctruction mimicking the RAII idiom
 */
template <typename Type, typename LockType>
class LockingPtr {
 public:
  using MutexType = typename LockType::mutex_type;

 public:
  LockingPtr(Type &value, MutexType &mutex) : _value{value}, _lock{mutex} {}

  LockingPtr(const LockingPtr<Type, LockType> &) = delete;
  LockingPtr(LockingPtr<Type, LockType> &&) = default;

  LockingPtr<Type, LockType> &operator=(const LockingPtr<Type, LockType> &) = delete;
  LockingPtr<Type, LockType> &operator=(LockingPtr<Type, LockType> &&) = default;

  Type &operator*() { return _value; }
  const Type &operator*() const { return _value; }

  Type *operator->() { return &_value; }
  const Type *operator->() const { return &_value; }

 private:
  Type &_value;
  LockType _lock;
};

template <typename Type>
using SharedLockLockingPtr = LockingPtr<Type, std::shared_lock<std::shared_mutex>>;

template <typename Type>
using UniqueLockLockingPtr = LockingPtr<Type, std::unique_lock<std::shared_mutex>>;

}  // namespace
