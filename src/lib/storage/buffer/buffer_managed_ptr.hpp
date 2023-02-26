#pragma once

#include <climits>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <variant>
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

namespace detail {

struct PageIDOffsetAddress {
  size_t page_id : 32;
  size_t offset : 32;
};

static_assert(sizeof(PageIDOffsetAddress) == 8);

}  // namespace detail

template <typename PointedType>
class BufferManagedPtr {
  using OutsideAddress = std::uintptr_t;
  using EmptyAddress = std::monostate;
  using PageIDOffsetAddress = detail::PageIDOffsetAddress;
  using Addressing = std::variant<EmptyAddress, OutsideAddress, PageIDOffsetAddress>;

 public:
  using pointer = PointedType*;
  using reference = typename add_reference<PointedType>::type;
  using element_type = PointedType;
  using value_type = std::remove_cv_t<PointedType>;  // TODO: is std::remove_cv_t a good idea?
  using difference_type = std::ptrdiff_t;            // TODO: Remove page offfset
  using iterator_category = std::random_access_iterator_tag;
  using PageIDAndOffset = std::pair<PageID, difference_type>;

  template <typename U>
  friend class BufferManagedPtr;

  // TODO: This does not compile when unordered map/set iteratorsare used
  // using iterator_category = std::contiguous_iterator_tag;
  // it seems like boost iterator_enable_if_tag does not it up
  // using iterator_concept = std::random_access_iterator_tag;  // TODO: contiguous_iterator_tag
  // TODO: introduce custom iterator type, that keeps the pointer/page in mem all the time, but works with raw pointer
  // Check offset pointer for alignment
  // Offset type it not difference type
  // TODO: Offset = 0 should be invalid, check other type for that when transforming into pinter and back
  // https://github.com/darwin/boost/blob/master/boost/interprocess/offset_ptr.hpp
  // https://www.youtube.com/watch?v=_nIET46ul6E
  // Segment ptr uses pointer swizzlhttps://github.com/boostorg/interprocess/blob/4403b201bef142f07cdc43f67bf6477da5e07fe3/include/boost/interprocess/detail/intersegment_ptr.hpp#L611
  BufferManagedPtr(pointer ptr = 0) {
    set_from_ptr(ptr);
  }

  BufferManagedPtr(const BufferManagedPtr& ptr) : _addressing(ptr._addressing) {}

  template <typename U>
  BufferManagedPtr(const BufferManagedPtr<U>& other) : _addressing(other._addressing) {}

  template <typename T>
  BufferManagedPtr(T* ptr) {
    set_from_ptr(ptr);
  }

  explicit BufferManagedPtr(const PageID page_id, difference_type offset) {
    _addressing = PageIDOffsetAddress{page_id, static_cast<size_t>(offset)};
  }

  pointer operator->() const {
    return get();
  }

  reference operator*() const {
    // TODO: Check if pid, and try to reduce branching
    pointer p = this->get();
    reference r = *p;
    return r;
  }

  reference operator[](std::ptrdiff_t idx) const {
    return get()[idx];
  }

  bool operator!() const {
    return std::holds_alternative<EmptyAddress>(_addressing);
  }

  BufferManagedPtr operator+(std::ptrdiff_t offset) const {
    auto new_addressing = _addressing;
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&new_addressing)) {
      *outside_addressing += offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<PageIDOffsetAddress>(&new_addressing)) {
      page_id_offset->offset += offset * difference_type(sizeof(PointedType));
    }
    return BufferManagedPtr(new_addressing);
  }

  BufferManagedPtr operator-(std::ptrdiff_t offset) const {
    auto new_addressing = _addressing;
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&new_addressing)) {
      *outside_addressing -= offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<PageIDOffsetAddress>(&new_addressing)) {
      page_id_offset->offset -= offset * difference_type(sizeof(PointedType));
    }
    return BufferManagedPtr(new_addressing);
  }

  BufferManagedPtr& operator+=(difference_type offset) noexcept {
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&_addressing)) {
      *outside_addressing += offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<PageIDOffsetAddress>(&_addressing)) {
      page_id_offset->offset += offset * difference_type(sizeof(PointedType));
    }
    return *this;
  }

  BufferManagedPtr& operator-=(difference_type offset) noexcept {
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&_addressing)) {
      *outside_addressing -= offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<PageIDOffsetAddress>(&_addressing)) {
      page_id_offset->offset -= offset * difference_type(sizeof(PointedType));
    }
    return *this;
  }

  BufferManagedPtr& operator=(const BufferManagedPtr& ptr) {
    _addressing = ptr._addressing;
    return *this;
  }

  BufferManagedPtr& operator=(pointer from) {
    set_from_ptr(from);
    return *this;
  }

  template <typename T2>
  BufferManagedPtr& operator=(const BufferManagedPtr<T2>& ptr) {
    _addressing = ptr._addressing;
    return *this;
  }

  explicit operator bool() const noexcept {
    return !std::holds_alternative<EmptyAddress>(_addressing);
  }

  pointer get() const {
    return static_cast<pointer>(this->get_pointer());
  }

  static BufferManagedPtr pointer_to(reference r) {
    return BufferManagedPtr(&r);
  }

  friend bool operator==(const BufferManagedPtr& ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1.get() == ptr2.get();
  }

  friend bool operator==(pointer ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1 == ptr2.get();
  }

  BufferManagedPtr& operator++(void) noexcept {
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&_addressing)) {
      *outside_addressing += difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<PageIDOffsetAddress>(&_addressing)) {
      page_id_offset->offset += difference_type(sizeof(PointedType));
    }
    return *this;
  }

  BufferManagedPtr operator++(int) noexcept {
    const auto temp = BufferManagedPtr(*this);
    ++*this;
    return temp;
  }

  BufferManagedPtr& operator--(void) noexcept {
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&_addressing)) {
      *outside_addressing -= difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<PageIDOffsetAddress>(&_addressing)) {
      page_id_offset->offset -= difference_type(sizeof(PointedType));
    }
    return *this;
  }

  friend bool operator<(const BufferManagedPtr& ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1.get() < ptr2.get();
  }

  friend bool operator<(pointer& ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1 < ptr2.get();
  }

  friend bool operator<(const BufferManagedPtr& ptr1, pointer ptr2) noexcept {
    return ptr1.get() < ptr2;
  }

  void* get_pointer() const {
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&_addressing)) {
      return (void*)*outside_addressing;
    } else if (const auto page_id_offset = std::get_if<PageIDOffsetAddress>(&_addressing)) {
      const auto page_id = PageID{page_id_offset->page_id};
      // TODO: If pinned, this is not needed
      // TODO: What happens if page is deleted? Pointer should become null
      const auto page = BufferManager::get_global_buffer_manager().get_page(page_id);
      return page->data() + page_id_offset->offset;
    } else if (std::holds_alternative<EmptyAddress>(_addressing)) {
      return nullptr;
    } else {
      Fail("Cannot get value from addressing variant");
    }
  }

  // TODO: Return a guard to ensure unpinning. check pointer type
  void pin() const {
    const auto page_id = PageID{std::get<PageIDOffsetAddress>(_addressing).page_id};
    BufferManager::get_global_buffer_manager().pin_page(page_id);
  }

  void unpin(bool dirty) const {
    const auto page_id = PageID{std::get<PageIDOffsetAddress>(_addressing).page_id};
    BufferManager::get_global_buffer_manager().unpin_page(page_id, dirty);
  }

  PageID get_page_id() const {
    return PageID{std::get<PageIDOffsetAddress>(_addressing).page_id};
  }

  difference_type get_offset() const {
    return std::get<PageIDOffsetAddress>(_addressing).offset;
  }

 private:
  Addressing _addressing;

  template <typename T>
  void set_from_ptr(T* ptr) {
    if (ptr) {
      const auto [page_id, offset] = BufferManager::get_global_buffer_manager().get_page_id_and_offset_from_ptr(
          reinterpret_cast<const void*>(ptr));
      if (page_id == INVALID_PAGE_ID) {
        _addressing = OutsideAddress(ptr);
      } else {
        _addressing = PageIDOffsetAddress{page_id, static_cast<size_t>(offset)};
      }
    } else {
      _addressing = EmptyAddress{};
    }
  }

  BufferManagedPtr(const Addressing& addressing) : _addressing(addressing) {}
};

template <class T1, class T2>
inline bool operator==(const BufferManagedPtr<T1>& ptr1, const BufferManagedPtr<T2>& ptr2) {
  return ptr1.get() == ptr2.get();
}

template <class T1, class T2>
inline bool operator!=(const BufferManagedPtr<T1>& ptr1, const BufferManagedPtr<T2>& ptr2) {
  return ptr1.get() != ptr2.get();
}

template <class T>
inline BufferManagedPtr<T> operator+(std::ptrdiff_t diff, const BufferManagedPtr<T>& right) {
  return right + diff;
}

template <class T, class T2>
inline std::ptrdiff_t operator-(const BufferManagedPtr<T>& pt, const BufferManagedPtr<T2>& ptr2) {
  return pt.get() - ptr2.get();
}

template <class T1, class T2>
inline bool operator<=(const BufferManagedPtr<T1>& ptr1, const BufferManagedPtr<T2>& ptr2) {
  return ptr1.get() <= ptr2.get();
}

template <class T>
inline void swap(BufferManagedPtr<T>& ptr1, BufferManagedPtr<T>& ptr2) {
  auto temp = ptr1.get();
  ptr1 = ptr2;
  ptr2 = temp;
}

/**
 * A Pin Guard ensure that that a pointer/page is unpinned when it goes out of scope. This garantuees exceptions safetiness. It works like std::lock_guard.
*/
template <class T>
class PinGuard {
 public:
  explicit PinGuard(BufferManagedPtr<T>& ptr, const bool dirty) : _dirty(dirty), _ptr(ptr) {
    _ptr.pin();
  }

  ~PinGuard() {
    _ptr.unpin(_dirty);
  }

 private:
  BufferManagedPtr<T>& _ptr;
  const bool _dirty;
};

}  // namespace hyrise

namespace std {

template <class T>
struct hash<hyrise::BufferManagedPtr<T>> {
  size_t operator()(const hyrise::BufferManagedPtr<T>& ptr) const noexcept {
    return std::hash<typename hyrise::BufferManagedPtr<T>::Addressing>(ptr._addressing);
  }
};
}  // namespace std