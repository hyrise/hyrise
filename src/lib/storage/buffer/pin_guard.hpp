#pragma once

#include <tbb/concurrent_hash_map.h>
#include <boost/container/small_vector.hpp>
#include <set>
#include <type_traits>
#include "storage/buffer/buffer_pool_allocator_observer.hpp"
#include "storage/buffer/memory_resource.hpp"
#include "storage/buffer/types.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "storage/lz4_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/bitpacking/bitpacking_vector.hpp"
#include "types.hpp"

namespace hyrise {

template <AccessIntent accessIntent>
struct PinnedFrames : public Noncopyable {
 public:
  void add_pin(const FramePtr& frame) {
    if (frame->is_invalid()) {
      return;
    }
    _pins.push_back(frame);
    get_buffer_manager_memory_resource()->pin(frame);
    lock(frame);
    // DebugAssert(frame->is_resident(), "Is resident");
  }

  void remove_pin(const FramePtr& frame) {
    auto it = std::find(_pins.begin(), _pins.end(), frame);
    if (it != _pins.end()) {
      get_buffer_manager_memory_resource()->unpin(frame, false);
      unlock(frame);
      _pins.erase(it);
    }
  }

  bool has_pinned(const FramePtr& frame) {
    auto it = std::find(_pins.begin(), _pins.end(), frame);
    return it != _pins.end();
  }

  void lock(const FramePtr& frame) {
    if constexpr (accessIntent == AccessIntent::Read) {
      // TODO frame->lock_shared();
    } else if constexpr (accessIntent == AccessIntent::Write) {
      // frame->lock_exclusive();
    } else {
      Fail("Not implemented.");
    }
  }

  void unlock(const FramePtr& frame) {
    if constexpr (accessIntent == AccessIntent::Read) {
      // frame->unlock_shared();
    } else if constexpr (accessIntent == AccessIntent::Write) {
      // frame->unlock_exclusive();
    } else {
      Fail("Not implemented.");
    }
  }

  ~PinnedFrames() {
    for (const auto& frame : _pins) {
      get_buffer_manager_memory_resource()->unpin(frame, accessIntent == AccessIntent::Write);
      unlock(frame);
    }
  }

  // TODO: Maybe change to other container, and make it concurrent, use boost flat_set for faster inserts and delets?
  boost::container::small_vector<FramePtr, 5> _pins;
};

template <AccessIntent accessIntent>
struct FramePinGuard final : public PinnedFrames<accessIntent> {
  using PinnedFrames<accessIntent>::add_pin;
  using PinnedFrames<accessIntent>::has_pinned;

  template <typename T, typename = void>
  struct is_iterable : std::false_type {};

  template <typename T>
  struct is_iterable<T, std::void_t<decltype(std::begin(std::declval<T>())), decltype(std::end(std::declval<T>()))>>
      : std::true_type {};

  template <typename T, typename = std::enable_if_t<is_iterable<T>::value>>
  void add_vector_pins(const T& vector) {
    vector.for_each_ptr([&](auto& ptr) { add_pin(ptr.get_frame()); });

    // Ensure that all child frames are pinned for pmr_vector<pmr_string>
    if constexpr (std::is_same_v<std::remove_cv_t<T>, pmr_vector<pmr_string>>) {
      for (const auto& string : vector) {
        const auto string_frame = string.begin().get_frame();
        if (!string_frame) {
          continue;
        }

        if (has_pinned(string_frame) || string_frame->is_invalid()) {
          continue;
        }
        add_pin(string_frame);
      }
    }
  }

 public:
  template <typename T, typename = std::enable_if_t<is_iterable<T>::value>>
  FramePinGuard(T& object) {
    add_vector_pins(object);
  }

  template <typename T, typename = std::enable_if_t<is_iterable<T>::value>>
  FramePinGuard(const std::shared_ptr<T>& position_filter) {
    if (const auto vector = std::dynamic_pointer_cast<const RowIDPosList>(position_filter)) {
      add_vector_pins(*vector);
    };
  }

  template <>
  FramePinGuard(const BitPackingVector& object) {
    Fail("Not implemented");
    // add_vector_pins(object.data());
  }

  template <>
  FramePinGuard(const pmr_compact_vector& object) {
    Fail("Not implemented");
    // add_vector_pins(object.get());
  }

  template <typename T>
  FramePinGuard(const FixedWidthIntegerVector<T>& object) {
    add_vector_pins(object.data());
  }

  template <typename T>
  FramePinGuard(const ValueSegment<T>& segment) {
    add_vector_pins(segment.values());
  }

  template <typename T>
  FramePinGuard(const FrameOfReferenceSegment<T>& segment) {
    Fail("FrameOfReferenceSegment not implemented yet");
  }

  template <typename T>
  FramePinGuard(const RunLengthSegment<T>& segment) {
    Fail("RunLengthSegment not implemented yet");
  }

  template <typename T>
  FramePinGuard(const LZ4Segment<T>& segment) {
    Fail("LZ4Segment not implemented yet");
  }

  template <typename T>
  FramePinGuard(const FixedStringDictionarySegment<T>& segment) {
    Fail("FixedStringDictionarySegment not implemented yet");
  }

  FramePinGuard(const ReferenceSegment& segment) {
    Fail("ReferenceSegment not implemented yet");
  }

  template <typename T>
  FramePinGuard(const DictionarySegment<T>& segment) {
    const auto attribute_vector_frame = segment.attribute_vector();
    // add_vector_pins(*attribute_vector_frame);

    const auto frame = segment.dictionary();
    add_vector_pins(*frame);
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
  struct Observer final : public PinnedFrames<AccessIntent::Write>, public BufferPoolAllocatorObserver {
    void on_allocate(const FramePtr& frame) override {
      // TODO: solve pinning issue
      if (!has_pinned(frame)) {
        add_pin(frame);
      }
    }

    void on_deallocate(const FramePtr& frame) override {
      // remove_pin(frame);
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