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

template <typename PointedType>  // TODO: Introduce tag that can do ref counting
class BufferPtr {
 public:
  // Used to mark the BufferPtr as freshly allocated after allocation
  struct AllocTag {};

  using pointer = PointedType*;

  using reference = typename add_reference<PointedType>::type;
  using element_type = PointedType;
  using value_type = std::remove_cv_t<PointedType>;
  using difference_type = std::ptrdiff_t;
  using iterator_category = std::random_access_iterator_tag;
  // using IsConst = std::is_const<PointedType>::value;

  template <typename U>
  friend class BufferPtr;
  friend class BufferManager;
  template <typename U>
  friend class BufferPoolAllocator;

  // TODO: Ensure Pinning before every access

  // using iterator_category = std::contiguous_iterator_tag;
  // it seems like boost iterator_enable_if_tag does not it up
  // TODO: Rename
  // using iterator_concept = std::random_access_iterator_tag;  // TODO: contiguous_iterator_tag
  // TODO: introduce custom iterator type, that keeps the pointer/page in mem all the time, but works with raw pointer
  // Check offset pointer for alignment
  // Offset type it not difference type
  // TODO: Offset = 0 should be invalid, check other type for that when transforming into pinter and back
  // https://github.com/darwin/boost/blob/master/boost/interprocess/offset_ptr.hpp
  // https://www.youtube.com/watch?v=_nIET46ul6E
  // Segment ptr uses pointer swizzlhttps://github.com/boostorg/interprocess/blob/4403b201bef142f07cdc43f67bf6477da5e07fe3/include/boost/interprocess/detail/intersegment_ptr.hpp#L611

  BufferPtr(pointer ptr = 0) {
    find_frame_and_offset(ptr);
  }

  BufferPtr(const BufferPtr& other) : _frame(try_replace_frame(other._frame)), _ptr_or_offset(other._ptr_or_offset) {}

  template <typename U>
  BufferPtr(const BufferPtr<U>& other)
      : _frame(try_replace_frame(other._frame)), _ptr_or_offset(other._ptr_or_offset) {}

  BufferPtr(BufferPtr&& other)
      : _frame(try_replace_frame(other._frame)), _ptr_or_offset(std::move(other._ptr_or_offset)) {}

  template <typename U>
  BufferPtr(BufferPtr<U>&& other)
      : _frame(try_replace_frame(other._frame)), _ptr_or_offset(std::move(other._ptr_or_offset)) {}

  template <typename T>
  BufferPtr(T* ptr) {
    find_frame_and_offset(ptr);
  }

  template <typename U>
  BufferPtr(const BufferPtr<U>& other, const AllocTag& tag)
      : _frame(other._frame), _ptr_or_offset(other._ptr_or_offset) {}

  explicit BufferPtr(Frame* frame, const size_t offset, const AllocTag& tag) : _frame(frame), _ptr_or_offset(offset) {}

  pointer operator->() const {
    return get();
  }

  reference operator*() const {
    pointer p = this->get();
    reference r = *p;
    return r;
  }

  reference operator[](std::ptrdiff_t idx) {
    return get()[idx];
  }

  const reference operator[](std::ptrdiff_t idx) const {
    return get()[idx];
  }

  bool operator!() const {
    return !_frame->data && !_ptr_or_offset;
  }

  BufferPtr operator+(difference_type offset) const noexcept {
    auto new_ptr = BufferPtr(*this);
    new_ptr.inc_offset(offset * difference_type(sizeof(PointedType)));
    return new_ptr;
  }

  BufferPtr operator-(difference_type offset) const noexcept {
    auto new_ptr = BufferPtr(*this);
    new_ptr.dec_offset(offset * difference_type(sizeof(PointedType)));
    return new_ptr;
  }

  BufferPtr& operator+=(difference_type offset) noexcept {
    inc_offset(offset * difference_type(sizeof(PointedType)));
    return *this;
  }

  BufferPtr& operator-=(difference_type offset) noexcept {
    dec_offset(offset * difference_type(sizeof(PointedType)));
    return *this;
  }

  BufferPtr& operator=(BufferPtr other) {
    swap(*this, other);
    return *this;
  }

  friend void swap(BufferPtr& first, BufferPtr& second) {
    std::swap(first._frame, second._frame);
    std::swap(first._ptr_or_offset, second._ptr_or_offset);
  }

  BufferPtr& operator=(pointer from) {
    find_frame_and_offset(from);
    return *this;
  }

  template <typename T2>
  BufferPtr& operator=(const BufferPtr<T2>& other) {
    swap(*this, other);
    return *this;
  }

  explicit operator bool() const noexcept {
    return _frame->data || _ptr_or_offset;
  }

  pointer get() const {
    return reinterpret_cast<pointer>(_frame->data + _ptr_or_offset);
  }

  inline friend bool operator==(const BufferPtr& ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1.get() == ptr2.get();
  }

  inline friend bool operator==(pointer ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1 == ptr2.get();
  }

  inline friend bool operator!=(const BufferPtr& ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1.get() != ptr2.get();
  }

  BufferPtr& operator++(void) noexcept {
    inc_offset(difference_type(sizeof(PointedType)));
    return *this;
  }

  BufferPtr operator++(int) noexcept {
    const auto temp = BufferPtr(*this);
    inc_offset(difference_type(sizeof(PointedType)));
    return temp;
  }

  BufferPtr& operator--(void) noexcept {
    dec_offset(difference_type(sizeof(PointedType)));
    return *this;
  }

  friend bool operator<(const BufferPtr& ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1.get() < ptr2.get();
  }

  friend bool operator<(pointer& ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1 < ptr2.get();
  }

  friend bool operator<(const BufferPtr& ptr1, pointer ptr2) noexcept {
    return ptr1.get() < ptr2;
  }

  FramePtr get_frame() const {
    if (_frame->page_type == PageType::Invalid) {
      return nullptr;
    }
    return FramePtr(_frame);
  }

  std::uintptr_t get_offset() const {
    return _ptr_or_offset;
  }

  size_t get_frame_ref_count() const {
    return _frame->_internal_ref_count();
  }

  static BufferPtr pointer_to(reference ref) {
    return BufferPtr(&ref);
  }

  Frame* _frame;
  std::uintptr_t _ptr_or_offset = 0;

  template <class T1, class T2>
  friend bool operator!=(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2);

  template <class T1, class T2>
  friend bool operator==(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2);

  void inc_offset(difference_type bytes) noexcept {
    _ptr_or_offset += bytes;
  }

  void dec_offset(difference_type bytes) noexcept {
    _ptr_or_offset -= bytes;
  }

  Frame* try_replace_frame(Frame* frame) {
    // TODO: Check for pin status
    // Before using a container with this pointer, we assume that a PinGuard was used.
    // Thus, either the DRAM or the NUMA frame is resident. However, the current frame is this ptr might not be best to use.
    // E.g. If the sibling frame is a DRAM frame, we would actually prefer it for performance reason

    if (frame->page_type == PageType::Invalid) {
      // Ignore invalid frames
      return frame;
    }

    auto numa_frame = frame->page_type == PageType::Numa ? frame : frame->sibling_frame.get();
    const auto is_numa_resident = numa_frame != nullptr && numa_frame->is_resident();

    auto dram_frame = frame->page_type == PageType::Dram ? frame : frame->sibling_frame.get();
    const auto is_dram_resident = dram_frame != nullptr && dram_frame->is_resident();

    if (is_dram_resident) {
      // DebugAssert(dram_frame->is_pinned(), "Frame should be pinned");
      return dram_frame;
    } else if (is_numa_resident) {
      // DebugAssert(numa_frame->is_pinned(), "Frame should be pinned");
      return numa_frame;
    } else {
      return frame;
      // Fail("Found non resident frame");
    }
  }

  template <typename T>
  void find_frame_and_offset(T* ptr) {
    _ptr_or_offset = reinterpret_cast<std::uintptr_t>(ptr);
    if (ptr) {
      const auto [frame, offset] =
          get_buffer_manager_memory_resource()->find_frame_and_offset(reinterpret_cast<const void*>(ptr));
      _frame = try_replace_frame(frame);
      if (frame->page_type != PageType::Invalid) {
        _ptr_or_offset = offset;
      }
      return;
    } else {
      _frame = &get_buffer_manager_memory_resource()->DUMMY_FRAME;
    }
  }
};

template <class T1, class T2>
inline bool operator==(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2) {
  return ptr1.get() == ptr2.get();
}

template <class T1, class T2>
inline bool operator!=(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2) {
  return ptr1.get() != ptr2.get();
}

template <class T>
inline BufferPtr<T> operator+(std::ptrdiff_t diff, const BufferPtr<T>& right) {
  return right + diff;
}

template <class T, class T2>
inline std::ptrdiff_t operator-(const BufferPtr<T>& ptr1, const BufferPtr<T2>& ptr2) {
  return ptr1.get() - ptr2.get();
}

template <class T1, class T2>
inline bool operator<=(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2) {
  return ptr1.get() <= ptr2.get();
}

}  // namespace hyrise
