#pragma once

#include <tbb/concurrent_hash_map.h>
#include <boost/container/small_vector.hpp>
#include "hyrise.hpp"

namespace hyrise {

// This is a concept that checks whether a type has a data() method which return value can be converted to void*.
template <typename T>
concept HasDataAccessor = requires(T t) {
  { t.data() } -> std::convertible_to<void*>;
};

// Container for page ids. We use a small_vector to avoid heap allocations for the common case of a single page id.
using PageIDContainer = boost::container::small_vector<PageID, 1>;

// Default implementation of the Pinnable type trait. This implementation is used for types that do not have a data accessor.
template <typename T>
struct Pinnable {
  static PageIDContainer get_page_ids(const BufferManager& buffer_manager, const T& pinnable) {
    Fail("Pinnable type not supported. Please implement an explicit specialization of Pinnable<T> for your type.");
  }
};

// Type trait that returns the page ids of a pinnable object with a data accessor e.g. pmr_vector.
template <HasDataAccessor T>
struct Pinnable<T> {
  static PageIDContainer get_page_ids(const BufferManager& buffer_manager, const T& pinnable) {
    const auto page_id = buffer_manager.find_page(pinnable.data());
    if (page_id.valid()) {
      return PageIDContainer{page_id};
    }
    return PageIDContainer{};
  }
};

// TODO: Const buffer manager ref
template <typename T>
struct ExclusivePinGuard final : public Noncopyable {
  explicit ExclusivePinGuard(const T& t, BufferManager& buffer_manager = Hyrise::get().buffer_manager)
      : _buffer_manager(buffer_manager) {
    _page_ids = Pinnable<T>::get_page_ids(_buffer_manager, t);
    for (const auto page_id : _page_ids) {
      buffer_manager.pin_exclusive(page_id);
      Hyrise::get().buffer_manager.set_dirty(page_id);
    }
  }

  ~ExclusivePinGuard() {
    for (const auto page_id : _page_ids) {
      Hyrise::get().buffer_manager.unpin_exclusive(page_id);
    }
  }

  PageIDContainer _page_ids;
  BufferManager& _buffer_manager;
};

template <typename T>
struct SharedPinGuard final : public Noncopyable {
  explicit SharedPinGuard(const T& object, BufferManager& buffer_manager = Hyrise::get().buffer_manager)
      : _buffer_manager(buffer_manager) {
    _page_ids = Pinnable<T>::get_page_ids(_buffer_manager, object);

    for (const auto page_id : _page_ids) {
      _buffer_manager.pin_shared(page_id);
    }
  }

  ~SharedPinGuard() {
    for (const auto page_id : _page_ids) {
      _buffer_manager.unpin_shared(page_id);
    }
  }

  PageIDContainer _page_ids;
  BufferManager& _buffer_manager;
};

}  // namespace hyrise