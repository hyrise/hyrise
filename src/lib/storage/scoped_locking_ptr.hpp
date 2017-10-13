#pragma once

#include <shared_mutex>

#include "types.hpp"

namespace opossum {

/**
 * Smart pointer that locks a given mutex on construction and
 * unlocks it on destruction mimicking the RAII idiom
 * Stores a reference to the wrapped value and therefore can’t be null
 * The advantage of using this pointer over for example a lock_guard is
 * that when used as a return type of a getter, it is impossible to access
 * the data structure without locking it.
 */
template <typename Type, typename LockType>
class ScopedLockingPtr : private Noncopyable {
 public:
  using MutexType = typename LockType::mutex_type;

 public:
  ScopedLockingPtr(Type& value, MutexType& mutex) : _value{value}, _lock{mutex} {}

  ScopedLockingPtr(ScopedLockingPtr<Type, LockType>&&) = default;

  ScopedLockingPtr<Type, LockType>& operator=(ScopedLockingPtr<Type, LockType>&&) = default;

  Type& operator*() { return _value; }
  const Type& operator*() const { return _value; }

  Type* operator->() { return &_value; }
  const Type* operator->() const { return &_value; }

 private:
  Type& _value;
  LockType _lock;
};

template <typename Type>
using SharedScopedLockingPtr = ScopedLockingPtr<Type, std::shared_lock<std::shared_mutex>>;

template <typename Type>
using UniqueScopedLockingPtr = ScopedLockingPtr<Type, std::unique_lock<std::shared_mutex>>;

}  // namespace opossum
