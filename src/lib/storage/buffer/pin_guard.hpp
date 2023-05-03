#pragma once

#include <boost/container/scoped_allocator.hpp>
#include <boost/container/small_vector.hpp>
#include <type_traits>
#include "storage/buffer/buffer_pool_allocator_observer.hpp"
#include "storage/buffer/memory_resource.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

template <typename T>
class BufferPoolAllocator;

class FramePinGuard : public Noncopyable {
 public:
  template <typename T>
  FramePinGuard(T& object, const AccessIntent access_intent = AccessIntent::Read) : _access_intent(access_intent) {
    object.begin().get_ptr().pin(*this);
  }

  FramePinGuard(const AccessIntent access_intent = AccessIntent::Read) : _access_intent(access_intent) {}

  void pin(std::shared_ptr<Frame> frame) {
    if (!_frame) {
      _frame = frame;
      get_buffer_manager_memory_resource()->pin(_frame);
    } else {
      Fail("Cannot pin two different frames");
    }
  }

  std::shared_ptr<Frame> get_frame() {
    return _frame;
  }

  AccessIntent get_access_intent() {
    return _access_intent;
  }

  ~FramePinGuard() {
    if (_frame) {
      const auto dirty = _access_intent == AccessIntent::Write;
      get_buffer_manager_memory_resource()->unpin(_frame, dirty);
    }
  }

 private:
  std::shared_ptr<Frame> _frame;
  const AccessIntent _access_intent;
};

/**
 * The AllocatorPinGuard observes all allocations and deallocations of a given allocator and pins the allocated pages during the life-time of the
 * AllocatorPinGuard including resizes of a data strcutures. The RAII-pattern ensure that all pages are unpinned when the AllocatorPinGuard is destroyed. 
 * The AllocatorPinGuard is not thread-safe!
*/
class AllocatorPinGuard : public Noncopyable {
  struct Observer : Noncopyable, BufferPoolAllocatorObserver {
    void on_allocate(const std::shared_ptr<SharedFrame>& frame) override {
      get_buffer_manager_memory_resource()->pin(frame->dram_frame);  // TODO: which frame?!
      _pins.push_back(frame);
    }

    void on_deallocate(const std::shared_ptr<SharedFrame>& frame) override {
      get_buffer_manager_memory_resource()->unpin(frame->dram_frame, false);  // TODO: which frame?!
      auto it = std::find(_pins.begin(), _pins.end(), frame);
      if (it != _pins.end()) {
        _pins.erase(it);
      }
    }

    ~Observer() {
      for (const auto& frame : _pins) {
        get_buffer_manager_memory_resource()->unpin(frame->dram_frame, true);
      }
    }

    // TODO: Maybe change to other container, and make it concurrent
    boost::container::small_vector<std::shared_ptr<SharedFrame>, 5> _pins;
  };

 public:
  template <class Allocator>
  AllocatorPinGuard(Allocator& allocator) : _observer(std::make_shared<Observer>()) {
    if constexpr (std::is_same_v<Allocator, BufferPoolAllocator<typename Allocator::value_type>>) {
      allocator.register_observer(_observer);
    } else if constexpr (std::is_same_v<Allocator, boost::container::scoped_allocator_adaptor<
                                                       BufferPoolAllocator<typename Allocator::value_type>>>) {
      allocator.outer_allocator().register_observer(_observer);
    } else {
      Fail("AllocatorPinGuard is not implemented for given allocator");
    }
  }

 private:
  std::shared_ptr<Observer> _observer;
};

}  // namespace hyrise