#pragma once

#include <tbb/concurrent_vector.h>  // NEEDEDINCLUDE

#include "polymorphic_allocator.hpp"

namespace opossum {

// We are not using PMR here because of the problems described in #281.
// Short version: The current TBB breaks with it, because it needs rebind.
// Once that works, replace the class below with
// using pmr_concurrent_vector = tbb::concurrent_vector<T, PolymorphicAllocator<T>>;
template <typename T>
class pmr_concurrent_vector : public tbb::concurrent_vector<T> {
 public:
  pmr_concurrent_vector(PolymorphicAllocator<T> alloc = {}) : pmr_concurrent_vector(0, alloc) {}  // NOLINT
  pmr_concurrent_vector(std::initializer_list<T> init_list, PolymorphicAllocator<T> alloc = {})
      : tbb::concurrent_vector<T>(init_list), _alloc(alloc) {}         // NOLINT
  pmr_concurrent_vector(size_t n, PolymorphicAllocator<T> alloc = {})  // NOLINT
      : pmr_concurrent_vector(n, T{}, alloc) {}
  pmr_concurrent_vector(size_t n, T val, PolymorphicAllocator<T> alloc = {})  // NOLINT
      : tbb::concurrent_vector<T>(n, val), _alloc(alloc) {}
  pmr_concurrent_vector(tbb::concurrent_vector<T> other, PolymorphicAllocator<T> alloc = {})  // NOLINT
      : tbb::concurrent_vector<T>(other), _alloc(alloc) {}
  pmr_concurrent_vector(const std::vector<T>& values, PolymorphicAllocator<T> alloc = {})  // NOLINT
      : tbb::concurrent_vector<T>(values.begin(), values.end()), _alloc(alloc) {}
  pmr_concurrent_vector(std::vector<T>&& values, PolymorphicAllocator<T> alloc = {})  // NOLINT
      : tbb::concurrent_vector<T>(std::make_move_iterator(values.begin()), std::make_move_iterator(values.end())),
        _alloc(alloc) {}

  template <class I>
  pmr_concurrent_vector(I first, I last, PolymorphicAllocator<T> alloc = {})
      : tbb::concurrent_vector<T>(first, last), _alloc(alloc) {}

  const PolymorphicAllocator<T>& get_allocator() const { return _alloc; }

 protected:
  PolymorphicAllocator<T> _alloc;
};

}  // namespace opossum
