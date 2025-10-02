#pragma once

#include <atomic>
#include <type_traits>
#include <utility>

namespace hyrise {

/**
 * @brief Copyable atomic wrapper
 *
 * Wrapper that implements the move constructor and assignment operator for std::atomic<>
 * Makes handling atomics in containers easier
 *
 * Attention: The following is not an atomic operation
 *   CopyableAtomic<int> a = 3, b = 4;
 *   a = b; // not atomic!
 *
 *   // internally this happens
 *   auto tmp = b.load();
 *   // execution might be interrupted here
 *   a.store(tmp);
 */
template <typename T>
class CopyableAtomic {
  static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable.");

 public:
  CopyableAtomic() noexcept = default;

  CopyableAtomic(const CopyableAtomic<T>& other) {
    _atomic.store(other._atomic.load());
  }

  explicit constexpr CopyableAtomic(T desired) noexcept : _atomic{desired} {}

  T operator=(T desired) noexcept {
    return _atomic.operator=(desired);
  }

  CopyableAtomic& operator=(const CopyableAtomic<T>& other) {
    _atomic.store(other._atomic.load());
    return *this;
  }

  bool is_lock_free() const {
    return _atomic.is_lock_free();
  }

  // NOLINTNEXTLINE(google-explicit-constructor,hicpp-explicit-conversions)
  operator T() const noexcept {
    return _atomic.load();
  }

  void store(T desired, std::memory_order order = std::memory_order_seq_cst) {
    _atomic.store(desired, order);
  }

  decltype(auto) load(std::memory_order order = std::memory_order_seq_cst) const {
    return _atomic.load(order);
  }

  template <typename... Args>
  decltype(auto) operator++(Args&&... args) {
    return _atomic.operator++(std::forward<Args>(args)...);
  }

  template <typename... Args>
  decltype(auto) operator--(Args&&... args) {
    return _atomic.operator--(std::forward<Args>(args)...);
  }

  template <typename... Args>
  bool exchange(Args&&... args) {
    return _atomic.exchange(std::forward<Args>(args)...);
  }

  template <typename... Args>
  bool compare_exchange_weak(Args&&... args) {
    return _atomic.compare_exchange_weak(std::forward<Args>(args)...);
  }

  template <typename... Args>
  bool compare_exchange_strong(Args&&... args) {
    return _atomic.compare_exchange_strong(std::forward<Args>(args)...);
  }

 private:
  std::atomic<T> _atomic;
};

}  // namespace hyrise
