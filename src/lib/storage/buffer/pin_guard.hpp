#pragma once

#include <tbb/concurrent_hash_map.h>
#include <boost/container/small_vector.hpp>
#include <set>
#include <type_traits>
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/buffer_pool_allocator_observer.hpp"
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
  void add_pin(const PageID page_id) {
    if (!page_id.valid()) {
      return;
    }

    if (has_pinned(page_id)) {
      return;
    }

    _pins.push_back(page_id);
    if constexpr (accessIntent == AccessIntent::Write) {
      BufferManager::get().pin_for_write(page_id);
    } else if constexpr (accessIntent == AccessIntent::Read) {
      BufferManager::get().pin_for_read(page_id);
    }
  }

  void remove_pin(const PageID page_id) {
    // auto it = std::find(_pins.begin(), _pins.end(), page_id);
    // if (it != _pins.end()) {
    //   _pins.erase(it);
    // }
  }

  bool has_pinned(const PageID page_id) {
    auto it = std::find(_pins.begin(), _pins.end(), page_id);
    return it != _pins.end();
  }

  ~PinnedFrames() {
    for (const auto page_id : _pins) {
      if constexpr (accessIntent == AccessIntent::Write) {
        BufferManager::get().set_dirty(page_id);
        BufferManager::get().unpin_for_write(page_id);
      } else if constexpr (accessIntent == AccessIntent::Read) {
        BufferManager::get().unpin_for_read(page_id);
      }
    }
  }

  std::vector<PageID> _pins;
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
    const auto page_id = BufferManager::get().find_page(vector.data());
    add_pin(page_id);

    // Ensure that all child frames are pinned for pmr_vector<pmr_string>
    if constexpr (std::is_same_v<std::remove_cv_t<T>, pmr_vector<pmr_string>>) {
      for (const auto& string : vector) {
        const auto string_page_id = BufferManager::get().find_page(string.data());
        add_pin(string_page_id);
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
    if (segment.is_nullable()) {
      add_vector_pins(segment.null_values());
    }
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
    Fail("DictionarySegment not implemented yet");
    const auto attribute_vector_frame = segment.attribute_vector();
    add_vector_pins(*attribute_vector_frame);

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
    void on_allocate(const PageID page_id) override {
      // TODO: solve pinning issue
      add_pin(page_id);
    }

    void on_deallocate(const PageID page_id) override {
      // remove_pin(page_id);
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