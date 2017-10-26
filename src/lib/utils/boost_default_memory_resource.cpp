#include <boost/container/pmr/memory_resource.hpp>
#include <boost/core/no_exceptions_support.hpp>

#include <cstddef>
#include <cstdlib>
#include <iostream>

namespace boost {
namespace container {
namespace pmr {

class default_resource_impl : public memory_resource {
 public:
  virtual void* do_allocate(std::size_t bytes, std::size_t alignment) { return std::malloc(bytes); }

  virtual void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) { std::free(p); }

  virtual bool do_is_equal(const memory_resource& other) const BOOST_NOEXCEPT { return &other == this; }
};

memory_resource* get_default_resource() BOOST_NOEXCEPT {
  static default_resource_impl default_resource_instance;
  return &default_resource_instance;
}

memory_resource* new_delete_resource() BOOST_NOEXCEPT { return get_default_resource(); }

memory_resource* set_default_resource(memory_resource* r) BOOST_NOEXCEPT {
  // Do nothing
  return get_default_resource();
}

}  // namespace pmr
}  // namespace container
}  // namespace boost
