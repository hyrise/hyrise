#include <cstddef>
#include <cstdlib>
#include <iostream>

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/core/no_exceptions_support.hpp>

namespace boost::container::pmr {

// We discourage manual memory management in Hyrise (such as malloc, or new), but in case of allocator/memory resource
// implementations, it is fine.
// NOLINTBEGIN(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,hicpp-no-malloc)

class default_resource_impl : public memory_resource {
 public:
  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    return std::malloc(bytes);
  }

  void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) override {
    std::free(pointer);
  }

  [[nodiscard]] bool do_is_equal(const memory_resource& other) const BOOST_NOEXCEPT override {
    return &other == this;
  }
};

memory_resource* get_default_resource() BOOST_NOEXCEPT {
  // Yes, this leaks. We have had SO many problems with the default memory resource going out of scope
  // before the other things were cleaned up that we decided to live with the leak, rather than
  // running into races over and over again.

  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,bugprone-unhandled-exception-at-new)
  static auto* default_resource_instance = new default_resource_impl();
  return default_resource_instance;
}

memory_resource* new_delete_resource() BOOST_NOEXCEPT {
  return get_default_resource();
}

memory_resource* set_default_resource(memory_resource* resource) BOOST_NOEXCEPT {
  // Do nothing
  return get_default_resource();
}

}  // namespace boost::container::pmr

// NOLINTEND(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,hicpp-no-malloc)
