#pragma once

#include <experimental/memory_resource>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

namespace opossum {

template <class T>
class StubPolymorphicAllocator : public std::experimental::pmr::polymorphic_allocator<T> {
 public:
  template <class U>
  struct rebind {
    typedef StubPolymorphicAllocator<U> other;
  };

  StubPolymorphicAllocator() {}
  template <class U>
  StubPolymorphicAllocator(const std::experimental::pmr::polymorphic_allocator<U>& alloc)
      : std::experimental::pmr::polymorphic_allocator<T>(alloc) {}
  template <class U>
  StubPolymorphicAllocator(const StubPolymorphicAllocator<U>& other)
      : std::experimental::pmr::polymorphic_allocator<T>(other) {}
  StubPolymorphicAllocator(std::experimental::pmr::memory_resource* m_resource)
      : std::experimental::pmr::polymorphic_allocator<T>(m_resource) {}
};

}  // namespace opossum