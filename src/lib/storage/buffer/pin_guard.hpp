#pragma once

#include <tbb/concurrent_hash_map.h>
#include <boost/container/small_vector.hpp>
#include "hyrise.hpp"

namespace hyrise {

// This is a concept that checks whether a type has a data() method that returns a void*.
template <typename T>
concept HasDataAccessor = requires(T t) {
  { t.data() } -> std::convertible_to<void*>;
};

// Container for page ids. We use a small_vector to avoid heap allocations for the common case of a single page id.
using PageIDContainer = boost::container::small_vector<PageID, 1>;

// Type trait that returns the page ids of a pinnable object with a data accessor.
template <HasDataAccessor T>
struct Pinnable {
  static PageIDContainer get_page_ids(const T& pinnable) {
    const auto page_id = Hyrise::get().buffer_manager.find_page(pinnable);
    if (page_id.valid()) {
      return PageIDContainer{page_id};
    }
    return PageIDContainer{};
  }
};

template <typename T>
struct ExclusivePinGuard final : public Noncopyable {
  explicit ExclusivePinGuard(const T& t) {
    _page_ids = Pinnable<T>::get_page_ids(t);
    for (const auto page_id : _page_ids) {
      Hyrise::get().buffer_manager.pin_exclusive(page_id);
      Hyrise::get().buffer_manager.set_dirty(page_id);
    }
  }

  ~ExclusivePinGuard() {
    for (const auto page_id : _page_ids) {
      Hyrise::get().buffer_manager.unpin_exclusive(page_id);
    }
  }

  PageIDContainer _page_ids;
};

template <typename T>
struct SharedPinGuard final : public Noncopyable {
  explicit SharedPinGuard(const T& t) {
    _page_ids = Pinnable<T>::get_page_ids(t);

    for (const auto page_id : _page_ids) {
      Hyrise::get().buffer_manager.pin_shared(page_id);
    }
  }

  ~SharedPinGuard() {
    for (const auto page_id : _page_ids) {
      Hyrise::get().buffer_manager.unpin_shared(page_id);
    }
  }

  PageIDContainer _page_ids;
};

}  // namespace hyrise