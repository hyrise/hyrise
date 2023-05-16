#pragma once

#include <tbb/concurrent_hash_map.h>
#include <boost/container/small_vector.hpp>
#include <set>
#include <type_traits>
#include "storage/buffer/buffer_pool_allocator_observer.hpp"
#include "storage/buffer/memory_resource.hpp"
#include "storage/buffer/types.hpp"
#include "types.hpp"

namespace hyrise {

template <AccessIntent accessIntent>
struct PinnedFrames : public Noncopyable {
 public:
  void add_pin(const FramePtr& frame) {
    if (!has_pinned(frame)) {
      _pins.push_back(frame);
      get_buffer_manager_memory_resource()->pin(frame);
    }
  }

  void remove_pin(const FramePtr& frame) {
    auto it = std::find(_pins.begin(), _pins.end(), frame);
    if (it != _pins.end()) {
      get_buffer_manager_memory_resource()->unpin(frame, false);
      _pins.erase(it);
    }
  }

  bool has_pinned(const FramePtr& frame) {
    auto it = std::find(_pins.begin(), _pins.end(), frame);
    return it != _pins.end();
  }

  ~PinnedFrames() {
    for (const auto& frame : _pins) {
      get_buffer_manager_memory_resource()->unpin(frame, accessIntent == AccessIntent::Write);
    }
  }

  // TODO: Maybe change to other container, and make it concurrent, use boost flat_set for faster inserts and delets?
  boost::container::small_vector<FramePtr, 5> _pins;
};

template <AccessIntent accessIntent>
struct FramePinGuard final : public PinnedFrames<accessIntent> {
  using PinnedFrames<accessIntent>::add_pin;

 public:
  template <typename T>
  FramePinGuard(T& object) {
    const auto& frame = object.begin().get_ptr().get_frame();

    add_pin(frame);

    // Ensure that all child frames are pinned for pmr_vector<pmr_string>
    if constexpr (std::is_same_v<std::remove_cv_t<T>, pmr_vector<pmr_string>>) {
      for (const auto& string : object) {
        if (const auto& string_frame = string.begin().get_frame()) {
          add_pin(string_frame);
        }
      }
    }
  }

  template <>
  FramePinGuard(const std::shared_ptr<const AbstractPosList>& position_filter) {
    if (const auto vector = std::dynamic_pointer_cast<const RowIDPosList>(position_filter)) {
      const auto& frame = vector->begin().get_ptr().get_frame();
      add_pin(frame);
    };
  }
};

using ReadPinGuard = FramePinGuard<AccessIntent::Read>;
using WritePinGuard = FramePinGuard<AccessIntent::Write>;

/**
 * The AllocatorPinGuard observes all allocations and deallocations of a given allocator and pins the allocated pages during the life-time of the
 * AllocatorPinGuard including resizes of a data strcutures. The RAII-pattern ensure that all pages are unpinned when the AllocatorPinGuard is destroyed. 
 * The AllocatorPinGuard is not thread-safe!
*/
class AllocatorPinGuard final : private Noncopyable {
  struct Observer : public PinnedFrames<AccessIntent::Write>, public BufferPoolAllocatorObserver {
    void on_allocate(const FramePtr& frame) override {
      add_pin(frame);
    }

    void on_deallocate(const FramePtr& frame) override {
      remove_pin(frame);
    }
  };

 public:
  template <class Allocator>
  AllocatorPinGuard(Allocator& allocator) : _observer(std::make_unique<Observer>()) {
    if constexpr (std::is_same_v<Allocator, BufferPoolAllocator<typename Allocator::value_type>>) {
      allocator.register_observer(_observer);
    } else if constexpr (std::is_same_v<Allocator, PolymorphicAllocator<typename Allocator::value_type>>) {
      allocator.outer_allocator().register_observer(_observer);
    } else if constexpr (std::is_base_of_v<Allocator, PolymorphicAllocator<typename Allocator::value_type>>) {
      allocator.outer_allocator().register_observer(_observer);
    } else {
      Fail("AllocatorPinGuard is not implemented for given allocator");
    }
  }

 private:
  std::shared_ptr<Observer> _observer;
};
}  // namespace hyrise