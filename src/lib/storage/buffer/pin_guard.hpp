#pragma once

#include <tbb/concurrent_hash_map.h>
#include <boost/container/small_vector.hpp>
#include "hyrise.hpp"

namespace hyrise {

struct ExclusivePinGuard final : public Noncopyable {
  explicit ExclusivePinGuard(const PageID page_id, const BufferManager* buffer_manager = &Hyrise::get().buffer_manager)
      : page_id(page_id), buffer_manager(buffer_manager) {
    if (page_id.valid()) {
      buffer_manager->pin_exclusive(page_id);
      buffer_manager->mark_dirty(page_id);
    }
  }

  ~ExclusivePinGuard() {
    if (page_id.valid()) {
      buffer_manager->unpin_exclusive(page_id);
    }
  }

  const PageID page_id;
  const BufferManager* buffer_manager;
};

struct SharedPinGuard final : public Noncopyable {
  explicit SharedPinGuard(const PageID page_id, const BufferManager* buffer_manager = &Hyrise::get().buffer_manager)
      : page_id(page_id), buffer_manager(buffer_manager) {
    if (page_id.valid()) {
      buffer_manager->pin_shared(page_id);
    }
  }

  ~SharedPinGuard() {
    if (page_id.valid()) {
      buffer_manager->unpin_shared(page_id);
    }
  }

  const PageID page_id;
  const BufferManager* buffer_manager;
};

template <typename PinGuardType>
struct VectorPinGuard : public Noncopyable {
  template <typename T, typename = void>
  struct data_accessor : std::false_type {
    using ValueType = void;
  };

  template <typename T>
  struct data_accessor<T, std::void_t<decltype(std::declval<T>().data())>> : std::true_type {
    using ValueType = std::remove_cv_t<decltype(std::declval<T>().data())>::value;
  };

  template <typename T, typename = std::enable_if_t<data_accessor<T>::value>>
  DataPin(T& object, const BufferManager* buffer_manager = &Hyrise::get().buffer_manager) : 
    _pin_guard(buffer_manager->find_page(object.data(), buffer_manager) {
  }

  boost::container::small_vector<PinGuardType, 3> _page_ids;
};

}  // namespace hyrise