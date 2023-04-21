#pragma once

#include <boost/container/small_vector.hpp>
#include <type_traits>
#include "hyrise.hpp"
#include "storage/buffer/buffer_pool_allocator.hpp"
#include "storage/buffer/buffer_ptr.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * The AllocatorPinGuard observes all allocations and deallocations of a given allocator and pins the allocated pages during the life-time of the
 * AllocatorPinGuard including resizes of a data strcutures. The RAII-pattern ensure that all pages are unpinned when the AllocatorPinGuard is destroyed. 
 * The AllocatorPinGuard is not thread-safe!
*/
template <typename Allocator>
class AllocatorPinGuard : public Noncopyable {
  struct Observer : Noncopyable, BufferPoolAllocatorObserver {
    void on_allocate(std::shared_ptr<Frame> frame) override {
      Hyrise::get().buffer_manager.pin(frame);
      _pins.push_back(frame);
    }

    void on_deallocate(std::shared_ptr<Frame> frame) override {
      Hyrise::get().buffer_manager.unpin(frame, false);
      _pins.erase(std::remove(_pins.begin(), _pins.end(), frame), _pins.end());
    }

    ~Observer() {
      for (const auto frame : _pins) {
        Hyrise::get().buffer_manager.unpin(frame, true);
      }
    }

    boost::container::small_vector<std::shared_ptr<Frame>, 5> _pins;
  };

 public:
  AllocatorPinGuard(Allocator& allocator) : _observer(std::make_shared<Observer>()) {
    if constexpr (std::is_same_v<Allocator, BufferPoolAllocator<typename Allocator::value_type>>) {
      allocator.register_observer(_observer);
    } else if constexpr (std::is_same_v<Allocator, PolymorphicAllocator<typename Allocator::value_type>>) {
      allocator.outer_allocator().register_observer(_observer);
    } else {
      Fail("AllocatorPinGuard is not implemented for given allocator");
    }
  }

 private:
  std::shared_ptr<Observer> _observer;
};

}  // namespace hyrise