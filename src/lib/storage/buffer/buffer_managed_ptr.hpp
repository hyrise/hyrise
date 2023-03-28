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

// TODUse BufferFrame* and PageID as union fields

struct UnswizzledAddress {
  size_t size_type : BITS_PAGE_SIZE_TYPES;                                              // 3 bits for size type
  size_t page_id : sizeof(size_t) * CHAR_BIT - BITS_PAGE_SIZE_TYPES - MAX_PAGE_OFFSET;  // 44 bits for the page_id
  size_t offset : MAX_PAGE_OFFSET;  // 17 bits for the maximum offset

  PageID get_page_id() const {
    return PageID{page_id};
  }

  PageSizeType get_size_type() const {
    return static_cast<PageSizeType>(size_type);
  }
};

inline bool operator==(const UnswizzledAddress& lhs, const UnswizzledAddress& rhs) {
  return lhs.page_id == rhs.page_id && lhs.offset == rhs.offset && lhs.size_type == rhs.size_type;
}

// The unswizzled address should have only 64 bits
static_assert(sizeof(UnswizzledAddress) == 8);

}  // namespace detail

template <typename PointedType>
class BufferManagedPtr {
  using OutsideAddress = std::uintptr_t;
  using EmptyAddress = std::monostate;
  using UnswizzledAddress = detail::UnswizzledAddress;
  using Addressing = std::variant<EmptyAddress, OutsideAddress, UnswizzledAddress>;

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
  BufferManagedPtr(pointer ptr = 0) : _buffer_manager(&BufferManager::get_global_buffer_manager()) {
    unswizzle(ptr);
    DebugAssert(_buffer_manager != nullptr, "BufferManager is null");
  }

  BufferManagedPtr(const BufferManagedPtr& other)
      : _addressing(other._addressing),
        _buffer_manager(other._buffer_manager ? other._buffer_manager : &BufferManager::get_global_buffer_manager()) {
    DebugAssert(_buffer_manager != nullptr, "BufferManager is null");
  }

  template <typename U>
  BufferManagedPtr(const BufferManagedPtr<U>& other)
      : _addressing(other._addressing),
        _buffer_manager(other._buffer_manager ? other._buffer_manager : &BufferManager::get_global_buffer_manager()) {
    DebugAssert(_buffer_manager != nullptr, "BufferManager is null");
  }

  template <typename T>
  BufferManagedPtr(T* ptr) : _buffer_manager(&BufferManager::get_global_buffer_manager()) {
    unswizzle(ptr);
    DebugAssert(_buffer_manager != nullptr, "BufferManager is null");
  }

  explicit BufferManagedPtr(const PageID page_id, const difference_type offset, const PageSizeType size_type)
      : _buffer_manager(&BufferManager::get_global_buffer_manager()) {
    DebugAssert(_buffer_manager != nullptr, "BufferManager is null");

    _addressing = UnswizzledAddress{static_cast<size_t>(size_type), page_id, static_cast<size_t>(offset)};
  }

  explicit BufferManagedPtr(BufferManager* buffer_manager, const PageID page_id, const difference_type offset,
                            const PageSizeType size_type)
      : _buffer_manager(buffer_manager) {
    DebugAssert(_buffer_manager != nullptr, "BufferManager is null");

    _addressing = UnswizzledAddress{static_cast<size_t>(size_type), page_id, static_cast<size_t>(offset)};
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
    } else if (const auto page_id_offset = std::get_if<UnswizzledAddress>(&new_addressing)) {
      page_id_offset->offset += offset * difference_type(sizeof(PointedType));
    }
    return BufferManagedPtr(new_addressing);
  }

  BufferManagedPtr operator-(std::ptrdiff_t offset) const {
    auto new_addressing = _addressing;
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&new_addressing)) {
      *outside_addressing -= offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<UnswizzledAddress>(&new_addressing)) {
      page_id_offset->offset -= offset * difference_type(sizeof(PointedType));
    }
    return BufferManagedPtr(new_addressing);
  }

  BufferManagedPtr& operator+=(difference_type offset) noexcept {
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&_addressing)) {
      *outside_addressing += offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<UnswizzledAddress>(&_addressing)) {
      page_id_offset->offset += offset * difference_type(sizeof(PointedType));
    }
    return *this;
  }

  BufferManagedPtr& operator-=(difference_type offset) noexcept {
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&_addressing)) {
      *outside_addressing -= offset * difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<UnswizzledAddress>(&_addressing)) {
      page_id_offset->offset -= offset * difference_type(sizeof(PointedType));
    }
    return *this;
  }

  BufferManagedPtr& operator=(const BufferManagedPtr& ptr) {
    _addressing = ptr._addressing;
    _buffer_manager = ptr._buffer_manager;
    DebugAssert(_buffer_manager != nullptr, "BufferManager is null");

    return *this;
  }

  BufferManagedPtr& operator=(pointer from) {
    unswizzle(from);
    DebugAssert(_buffer_manager != nullptr, "BufferManager is null");
    return *this;
  }

  template <typename T2>
  BufferManagedPtr& operator=(const BufferManagedPtr<T2>& ptr) {
    _addressing = ptr._addressing;
    _buffer_manager = ptr._buffer_manager;
    DebugAssert(_buffer_manager != nullptr, "BufferManager is null");
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
    return ptr1._addressing == ptr2._addressing;
  }

  friend bool operator==(pointer ptr1, const BufferManagedPtr& ptr2) noexcept {
    return ptr1 == ptr2.get();
  }

  BufferManagedPtr& operator++(void) noexcept {
    if (const auto outside_addressing = std::get_if<OutsideAddress>(&_addressing)) {
      *outside_addressing += difference_type(sizeof(PointedType));
    } else if (const auto page_id_offset = std::get_if<UnswizzledAddress>(&_addressing)) {
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
    } else if (const auto page_id_offset = std::get_if<UnswizzledAddress>(&_addressing)) {
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
    } else if (const auto page_id_offset = std::get_if<UnswizzledAddress>(&_addressing)) {
      // TODO: If pinned, this is not needed
      // TODO: What happens if page is deleted? Pointer should become null
      const auto page = _buffer_manager->get_page(page_id_offset->get_page_id(), page_id_offset->get_size_type());
      return page + page_id_offset->offset;
    } else if (std::holds_alternative<EmptyAddress>(_addressing)) {
      return nullptr;
    } else {
      Fail("Cannot get value from addressing variant");
    }
  }

  // TODO: Return a guard to ensure unpinning. check pointer type
  void pin() const {
    if (const auto unswizzled = std::get_if<UnswizzledAddress>(&_addressing)) {
      _buffer_manager->pin_page(unswizzled->get_page_id(), unswizzled->get_size_type());
    }
  }

  void unpin(bool dirty) const {
    if (const auto unswizzled = std::get_if<UnswizzledAddress>(&_addressing)) {
      _buffer_manager->unpin_page(unswizzled->get_page_id(), unswizzled->get_size_type(), dirty);
    }
  }

  PageID get_page_id() const {
    if (const auto unswizzled = std::get_if<UnswizzledAddress>(&_addressing)) {
      return PageID{unswizzled->page_id};
    }
    return INVALID_PAGE_ID;
  }

  difference_type get_offset() const {
    if (const auto unswizzled = std::get_if<UnswizzledAddress>(&_addressing)) {
      return unswizzled->offset;
    }
    Fail("Cannot get offset from non-swizzled pointer");
  }

  PageSizeType get_size_type() const {
    if (const auto unswizzled = std::get_if<UnswizzledAddress>(&_addressing)) {
      return static_cast<PageSizeType>(unswizzled->size_type);
    }
    Fail("Cannot get offset from non-swizzled pointer");
  }

 private:
  Addressing _addressing = EmptyAddress{};
  BufferManager* _buffer_manager = nullptr;

  template <class T1, class T2>
  friend bool operator!=(const BufferManagedPtr<T1>& ptr1, const BufferManagedPtr<T2>& ptr2);

  template <class T1, class T2>
  friend bool operator==(const BufferManagedPtr<T1>& ptr1, const BufferManagedPtr<T2>& ptr2);

  template <typename T>
  void unswizzle(T* ptr) {
    if (ptr) {
      const auto [frame, offset] = _buffer_manager->unswizzle(reinterpret_cast<const void*>(ptr));
      if (frame) {
        _addressing =
            UnswizzledAddress{static_cast<size_t>(frame->size_type), frame->page_id, static_cast<size_t>(offset)};
        return;
      } else {
        _addressing = OutsideAddress(ptr);
        return;
      }
    }

    _addressing = EmptyAddress{};
  }

  BufferManagedPtr(const Addressing& addressing) : _addressing(addressing) {}
};

template <class T1, class T2>
inline bool operator==(const BufferManagedPtr<T1>& ptr1, const BufferManagedPtr<T2>& ptr2) {
  return ptr1._addressing == ptr2._addressing;
}

template <class T1, class T2>
inline bool operator!=(const BufferManagedPtr<T1>& ptr1, const BufferManagedPtr<T2>& ptr2) {
  return ptr1._addressing != ptr2._addressing;
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

}  // namespace hyrise

namespace std {
template <class T>
struct hash<hyrise::BufferManagedPtr<T>> {
  size_t operator()(const hyrise::BufferManagedPtr<T>& ptr) const noexcept {
    return std::hash<typename hyrise::BufferManagedPtr<T>::Addressing>(ptr._addressing);
  }
};
}  // namespace std