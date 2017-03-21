#pragma once

#include <atomic>
#include <type_traits>

namespace opossum {

/**
 * @brief Movable atomic wrapper
 *
 * Wrapper that implements the move constructor and assignment operator
 * Makes handling atomics in containers easier
 */
template <typename T>
class movable_atomic {
  static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable.");

 public:
  movable_atomic() noexcept = default;

  movable_atomic(const movable_atomic<T>&) = delete;

  movable_atomic(movable_atomic<T>&& rhs) noexcept { _atomic.store(rhs._atomic.load()); }

  constexpr movable_atomic(T desired) noexcept : _atomic{desired} {}

  T operator=(T desired) noexcept { return _atomic.operator=(desired); }

  movable_atomic& operator=(const movable_atomic<T>&) = delete;

  movable_atomic& operator=(movable_atomic<T>&& rhs) noexcept {
    _atomic.store(rhs._atomic.load());
    return *this;
  }

  bool is_lock_free() const { return _atomic.is_lock_free(); }

  operator T() const noexcept { return _atomic.load(); }

  void store(T desired, std::memory_order order = std::memory_order_seq_cst) { _atomic.store(desired, order); }

  decltype(auto) load(std::memory_order order = std::memory_order_seq_cst) const { return _atomic.load(order); }

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

}  // namespace opossum
