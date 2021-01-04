#include <boost/container/detail/dispatch_uses_allocator.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>

namespace opossum {

template <class T>
struct PolymorphicNoFreeAllocator {
 public:
  typedef T value_type;

  PolymorphicNoFreeAllocator() noexcept : m_resource(::boost::container::pmr::get_default_resource()) {}

  PolymorphicNoFreeAllocator(::boost::container::pmr::memory_resource* r)
      : m_resource(r) {}
  PolymorphicNoFreeAllocator(const PolymorphicNoFreeAllocator& other) : m_resource(other.m_resource) {}

  template <class U>
  PolymorphicNoFreeAllocator(const PolymorphicNoFreeAllocator<U>& other) noexcept : m_resource(other.resource()) {}

  PolymorphicNoFreeAllocator& operator=(const PolymorphicNoFreeAllocator& other) {
    m_resource = other.m_resource;
    return *this;
  }

  T* allocate(size_t n) {
    return static_cast<T*>(m_resource->allocate(n * sizeof(T), ::boost::move_detail::alignment_of<T>::value));
  }

  void deallocate(T* p, size_t n) noexcept {}

  template <typename U, class... Args>
  void construct(U* p, BOOST_FWD_REF(Args)... args) {
    ::boost::container::new_allocator<U> na;
    ::boost::container::dtl::dispatch_uses_allocator(na, this->resource(), p, ::boost::forward<Args>(args)...);
  }

  PolymorphicNoFreeAllocator select_on_container_copy_construction() const { return PolymorphicNoFreeAllocator(); }

  ::boost::container::pmr::memory_resource* resource() const { return m_resource; }

 private:
  ::boost::container::pmr::memory_resource* m_resource;
};

template <class T1, class T2>
bool operator==(const PolymorphicNoFreeAllocator<T1>& a, const PolymorphicNoFreeAllocator<T2>& b) noexcept {
  return *a.resource() == *b.resource();
}

template <class T1, class T2>
bool operator!=(const PolymorphicNoFreeAllocator<T1>& a, const PolymorphicNoFreeAllocator<T2>& b) noexcept {
  return *a.resource() != *b.resource();
}

}
