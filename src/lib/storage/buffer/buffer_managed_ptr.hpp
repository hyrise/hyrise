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

// #if !(defined(__x86_64__) || defined(_M_X64))
// // TODO: For Apple Silicon: https://opensource.apple.com/source/WTF/WTF-7601.1.46.42/wtf/Platform.h.auto.html
// Fail("Cannot use tagged pointer on current system");
// #endif

// static_assert(sizeof(std::size_t) == sizeof(std::uintptr_t));
// static_assert(sizeof(std::size_t) == sizeof(void*));
// static_assert(sizeof(void*) * CHAR_BIT == 64);

// const size_t is_outside_address = 0;
// const size_t is_page_id_and_offset = 1;

// struct outside_address {
//   std::uintptr_t address : 63;
//   std::uintptr_t tag : 1;
// };

// struct page_id_and_offset {
//   size_t offset : 31;
//   size_t page_id : 32;
//   size_t tag : 1;
// };

// union addressing_t {
//   std::uintptr_t raw;
//   outside_address outside;
//   page_id_and_offset buffer_manager;
// };

// static_assert(sizeof(uintptr_t) * CHAR_BIT == 64);

struct PageIDOffsetAddress {
  size_t page_id : 32;
  size_t offset : 32;
};

using OutsideAddress = std::uintptr_t;
using EmptyAddress = std::monostate;

using BufferManagedPtrAddressing =
    std::variant<detail::EmptyAddress, detail::OutsideAddress, detail::PageIDOffsetAddress>;
}  // namespace detail

template <typename PointedType>
class BufferManagedPtr {
  using Addressing = detail::BufferManagedPtrAddressing;

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
  // A lot of things are copied form offset_ptr
  // Pin and unp
  BufferManagedPtr(pointer ptr = 0) {
    // TODO: This function totally breaks right now
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
    _addressing = detail::PageIDOffsetAddress{page_id, static_cast<size_t>(offset)};
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
    return std::holds_alternative<detail::EmptyAddress>(_addressing);
  }

  BufferManagedPtr operator+(std::ptrdiff_t offset) const {
    auto new_addressing = _addressing;
    if (const auto outside_addressing = std::get_if<detail::OutsideAddress>(&new_addressing)) {
      *outside_addressing += offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<detail::PageIDOffsetAddress>(&new_addressing)) {
      page_id_offset->offset += offset * difference_type(sizeof(PointedType));
    }
    return BufferManagedPtr(new_addressing);
  }

  BufferManagedPtr operator-(std::ptrdiff_t offset) const {
    auto new_addressing = _addressing;
    if (const auto outside_addressing = std::get_if<detail::OutsideAddress>(&new_addressing)) {
      *outside_addressing -= offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<detail::PageIDOffsetAddress>(&new_addressing)) {
      page_id_offset->offset -= offset * difference_type(sizeof(PointedType));
    }
    return BufferManagedPtr(new_addressing);
  }

  BufferManagedPtr& operator+=(difference_type offset) noexcept {
    if (const auto outside_addressing = std::get_if<detail::OutsideAddress>(&_addressing)) {
      *outside_addressing += offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<detail::PageIDOffsetAddress>(&_addressing)) {
      page_id_offset->offset += offset * difference_type(sizeof(PointedType));
    }
    return *this;
  }

  BufferManagedPtr& operator-=(difference_type offset) noexcept {
    if (const auto outside_addressing = std::get_if<detail::OutsideAddress>(&_addressing)) {
      *outside_addressing -= offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<detail::PageIDOffsetAddress>(&_addressing)) {
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
    return !std::holds_alternative<detail::EmptyAddress>(_addressing);
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
    if (const auto outside_addressing = std::get_if<detail::OutsideAddress>(&_addressing)) {
      *outside_addressing += difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<detail::PageIDOffsetAddress>(&_addressing)) {
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
    if (const auto outside_addressing = std::get_if<detail::OutsideAddress>(&_addressing)) {
      *outside_addressing -= difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<detail::PageIDOffsetAddress>(&_addressing)) {
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
    if (const auto outside_addressing = std::get_if<detail::OutsideAddress>(&_addressing)) {
      return (void*)*outside_addressing;
    } else if (const auto page_id_offset = std::get_if<detail::PageIDOffsetAddress>(&_addressing)) {
      const auto page_id = PageID{page_id_offset->page_id};
      // TODO: If pinned, this is not needed
      // TODO: What happens if page is deleted? Pointer should become null
      const auto page = BufferManager::get_global_buffer_manager().get_page(page_id);
      return page->data() + page_id_offset->offset;
    } else {
      return nullptr;
    }
  }

  // TODO: Return a guard to ensure unpinning. check pointer type
  void pin() const {
    const auto page_id = PageID{std::get<detail::PageIDOffsetAddress>(_addressing).page_id};
    BufferManager::get_global_buffer_manager().pin_page(page_id);
  }

  void unpin(bool dirty) const {
    const auto page_id = PageID{std::get<detail::PageIDOffsetAddress>(_addressing).page_id};
    BufferManager::get_global_buffer_manager().unpin_page(page_id, dirty);
  }

  PageID get_page_id() const {
    return PageID{std::get<detail::PageIDOffsetAddress>(_addressing).page_id};
  }

  difference_type get_offset() const {
    return std::get<detail::PageIDOffsetAddress>(_addressing).offset;
  }

 private:
  Addressing _addressing;

  template <typename T>
  void set_from_ptr(T* ptr) {
    if (ptr) {
      const auto [page_id, offset] = BufferManager::get_global_buffer_manager().get_page_id_and_offset_from_ptr(
          reinterpret_cast<const void*>(ptr));
      if (page_id == INVALID_PAGE_ID) {
        _addressing = detail::OutsideAddress(ptr);
      } else {
        _addressing = detail::PageIDOffsetAddress{page_id, static_cast<size_t>(offset)};
      }
    } else {
      _addressing = detail::EmptyAddress{};
    }
  }

  // void set_null() {
  //   _addressing.raw = 0;
  // }

  // void set_outside_ptr(const std::uintptr_t ptr) {
  //   _addressing.outside.tag = detail::is_outside_address;
  //   _addressing.outside.address = ptr;
  // }

  // void set_page_id_and_offset(const PageID page_id, const difference_type offset) {
  //   _addressing.buffer_manager.tag = detail::is_page_id_and_offset;
  //   _addressing.buffer_manager.page_id = page_id;
  //   _addressing.buffer_manager.offset = offset;
  // }

  BufferManagedPtr(const Addressing addressing) : _addressing(addressing) {}
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
  typename BufferManagedPtr<T>::value_type* ptr = ptr1.get();
  ptr1 = ptr2;
  ptr2 = ptr;
}

}  // namespace hyrise

namespace std {

template <class T>
struct hash<hyrise::BufferManagedPtr<T>> {
  size_t operator()(const hyrise::BufferManagedPtr<T>& ptr) const noexcept {
    // TODO
    return 0;
  }
};

}  // namespace std