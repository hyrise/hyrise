#pragma once

#include <tbb/concurrent_hash_map.h>
#include <boost/container/small_vector.hpp>
#include <boost/core/demangle.hpp>
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

struct PinnedPageIds : public Noncopyable {
  template <typename T, typename = void>
  struct is_iterable : std::false_type {};

  template <typename T>
  struct is_iterable<T, std::void_t<decltype(std::begin(std::declval<T>())), decltype(std::end(std::declval<T>()))>>
      : std::true_type {};

  virtual void on_pin(const PageID page_id) = 0;
  virtual void on_unpin(const PageID page_id) = 0;

  PinnedPageIds() = default;

  template <typename T, typename = std::enable_if_t<is_iterable<T>::value>>
  PinnedPageIds(T& object) {
    add_vector_pins(object);
  }

  template <typename T, typename = std::enable_if_t<is_iterable<T>::value>>
  PinnedPageIds(const std::shared_ptr<T>& position_filter) {
    if (const auto vector = std::dynamic_pointer_cast<const RowIDPosList>(position_filter)) {
      add_vector_pins(*vector);
    }
  }

  PinnedPageIds(const BitPackingVector& object) {
    Fail("Not implemented");
    // add_vector_pins(object.data());
  }

  PinnedPageIds(const pmr_compact_vector& object) {
    Fail("Not implemented");

    add_vector_pins(object);
  }

  template <typename T>
  PinnedPageIds(const FixedWidthIntegerVector<T>& object) {
    add_vector_pins(object.data());
  }

  template <typename T>
  PinnedPageIds(const ValueSegment<T>& segment) {
    add_vector_pins(segment.values());
    if (segment.is_nullable()) {
      add_vector_pins(segment.null_values());
    }
  }

  template <typename T>
  PinnedPageIds(const FrameOfReferenceSegment<T>& segment) {
    Fail("FrameOfReferenceSegment not implemented yet");
  }

  template <typename T>
  PinnedPageIds(const RunLengthSegment<T>& segment) {
    Fail("RunLengthSegment not implemented yet");
  }

  template <typename T>
  PinnedPageIds(const LZ4Segment<T>& segment) {
    Fail("LZ4Segment not implemented yet");
  }

  template <typename T>
  PinnedPageIds(const FixedStringDictionarySegment<T>& segment) {
    Fail("FixedStringDictionarySegment not implemented yet");
  }

  PinnedPageIds(const ReferenceSegment& segment) {
    Fail("ReferenceSegment not implemented yet");
  }

  template <typename T>
  PinnedPageIds(const DictionarySegment<T>& segment) {
    resolve_compressed_vector_type(*segment.attribute_vector(), [&](auto& vector) { add_vector_pins(vector.data()); });
    add_vector_pins(*segment.dictionary());
  }

 protected:
  boost::container::small_vector<PageID, 3> _page_ids;

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

  void add_vector_pins(const pmr_compact_vector& vector) {
    const auto page_id = BufferManager::get().find_page(vector.get());
    add_pin(page_id);
  }

  void add_pin(const PageID page_id) {
    if (!page_id.valid()) {
      return;
    }

    if (has_pinned(page_id)) {
      return;
    }

    on_pin(page_id);
    _page_ids.push_back(page_id);
  }

  bool has_pinned(const PageID page_id) {
    auto it = std::find(_page_ids.begin(), _page_ids.end(), page_id);
    return it != _page_ids.end();
  }

  void unpin_all() {
    for (const auto page_id : _page_ids) {
      on_unpin(page_id);
    }
  }
};

// TODO: EqualDistinctCountHistogram

struct SharedReadPinGuard final : public PinnedPageIds {
  using PinnedPageIds::PinnedPageIds;

  void on_pin(const PageID page_id) override {
    BufferManager::get().pin_shared(page_id, AccessIntent::Read);
  }

  void on_unpin(const PageID page_id) override {
    BufferManager::get().unpin_shared(page_id);
  }

  ~SharedReadPinGuard() {
    PinnedPageIds::unpin_all();
  }
};

struct UnsafeSharedWritePinGuard final : public PinnedPageIds {
  using PinnedPageIds::PinnedPageIds;

  void on_pin(const PageID page_id) override {
    BufferManager::get().pin_shared(page_id, AccessIntent::Write);
  }

  void on_unpin(const PageID page_id) override {
    BufferManager::get().set_dirty(page_id);
    BufferManager::get().unpin_shared(page_id);
  }

  ~UnsafeSharedWritePinGuard() {
    PinnedPageIds::unpin_all();
  }
};

struct ExclusivePinGuard final : public PinnedPageIds {
  using PinnedPageIds::PinnedPageIds;

  void on_pin(const PageID page_id) override {
    BufferManager::get().pin_exclusive(page_id);
  }

  void on_unpin(const PageID page_id) override {
    BufferManager::get().set_dirty(page_id);
    BufferManager::get().unpin_exclusive(page_id);
  }

  ~ExclusivePinGuard() {
    PinnedPageIds::unpin_all();
  }
};

/**
 * The AllocatorPinGuard observes all allocations and deallocations of a given allocator and pins the allocated pages during the life-time of the
 * AllocatorPinGuard including resizes of a data strcutures. The RAII-pattern ensure that all pages are unpinned when the AllocatorPinGuard is destroyed. 
 * The AllocatorPinGuard is not thread-safe!
*/
class AllocatorPinGuard final : private Noncopyable {
  struct Observer final : public PinnedPageIds, public BufferPoolAllocatorObserver {
    using PinnedPageIds::add_pin;

    Observer() : PinnedPageIds{} {}

    void on_allocate(const PageID page_id) override;

    void on_deallocate(const PageID page_id) override;

    void on_pin(const PageID page_id) override;

    void on_unpin(const PageID page_id) override;

    ~Observer();
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