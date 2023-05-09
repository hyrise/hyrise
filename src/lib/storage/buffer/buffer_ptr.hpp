#pragma once

#include <climits>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <variant>
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/pin_guard.hpp"
#include "storage/buffer/types.hpp"

#include "utils/assert.hpp"

namespace hyrise {

template <typename PointedType>
class BufferPtr {
 public:
  using pointer = PointedType*;
  using reference = typename add_reference<PointedType>::type;
  using element_type = PointedType;
  using value_type = std::remove_cv_t<PointedType>;
  using difference_type = std::ptrdiff_t;
  using iterator_category = std::random_access_iterator_tag;
  using PtrOrOffset = std::uintptr_t;

  template <typename U>
  friend class BufferPtr;
  friend class BufferManager;

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
    unswizzle(ptr);
  }

  BufferPtr(const BufferPtr& other) : _frame(other._frame), _ptr_or_offset(other._ptr_or_offset) {
    assert_not_overflow();
  }

  template <typename U>
  BufferPtr(const BufferPtr<U>& other) : _frame(other._frame), _ptr_or_offset(other._ptr_or_offset) {
    assert_not_overflow();
  }

  template <typename T>
  BufferPtr(T* ptr) {
    unswizzle(ptr);
  }

  explicit BufferPtr(const FramePtr frame, const PtrOrOffset ptr_or_offset)
      : _frame(frame), _ptr_or_offset(ptr_or_offset) {
    assert_not_overflow();
  }

  pointer operator->() const {
    return get();
  }

  reference operator*() const {
    pointer p = this->get();
    if (!p) {
      Fail("Dereferencing null pointer");
    }
    reference r = *p;
    return r;
  }

  reference operator[](std::ptrdiff_t idx) {
    return get(AccessIntent::Write)[idx];
  }

  const reference operator[](std::ptrdiff_t idx) const {
    // TODO:mark dirty?
    return get(AccessIntent::Read)[idx];
  }

  bool operator!() const {
    return !_frame && !_ptr_or_offset;
  }

  BufferPtr operator+(difference_type offset) const noexcept {
    auto new_ptr = BufferPtr(_frame, _ptr_or_offset);
    new_ptr.inc_offset(offset * difference_type(sizeof(PointedType)));
    return new_ptr;
  }

  BufferPtr operator-(difference_type offset) const noexcept {
    auto new_ptr = BufferPtr(_frame, _ptr_or_offset);
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

  BufferPtr& operator=(const BufferPtr& ptr) {
    _frame = ptr._frame;
    _ptr_or_offset = ptr._ptr_or_offset;
    return *this;
  }

  BufferPtr& operator=(pointer from) {
    unswizzle(from);
    return *this;
  }

  template <typename T2>
  BufferPtr& operator=(const BufferPtr<T2>& ptr) {
    _frame = ptr._frame;
    _ptr_or_offset = ptr._ptr_or_offset;
    return *this;
  }

  explicit operator bool() const noexcept {
    // TODO: Load frame
    return _frame || _ptr_or_offset;
  }

  pointer get(const AccessIntent access_intent = AccessIntent::Read) const {
    return static_cast<pointer>(this->get_pointer(access_intent));
  }

  inline friend bool operator==(const BufferPtr& ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1._frame == ptr2._frame && ptr1._ptr_or_offset == ptr2._ptr_or_offset;
  }

  inline friend bool operator==(pointer ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1 == ptr2.get();
  }

  inline friend bool operator!=(const BufferPtr& ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1._frame != ptr2._frame || ptr1._ptr_or_offset != ptr2._ptr_or_offset;
  }

  BufferPtr& operator++(void) noexcept {
    inc_offset(difference_type(sizeof(PointedType)));
    return *this;
  }

  BufferPtr operator++(int) noexcept {
    const auto temp = BufferPtr(_frame, _ptr_or_offset);
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

  void* get_pointer(const AccessIntent access_intent = AccessIntent::Read) const {
    if (_frame) {
      const auto frame = make_resident(access_intent);
      return frame->data + _ptr_or_offset;
    } else {
      return (void*)_ptr_or_offset;
    }
  }

  pointer pin(FramePinGuard& guard) {
    auto frame = guard.get_frame();
    if (!frame) {
      frame = make_resident(guard.get_access_intent());
      guard.pin(frame);
    }
    return reinterpret_cast<pointer>(frame->data + _ptr_or_offset);
  }

  FramePtr make_resident(const AccessIntent access_intent) const {
    return get_buffer_manager_memory_resource()->make_resident(_frame, access_intent);
  }

  PtrOrOffset get_offset() const {
    return _ptr_or_offset;
  }

  FramePtr get_frame() const {
    return _frame;
  }

  static BufferPtr pointer_to(reference ref) {
    return BufferPtr(&ref);
  }

 private:
  FramePtr _frame;
  PtrOrOffset _ptr_or_offset = 0;

  template <class T1, class T2>
  friend bool operator!=(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2);

  template <class T1, class T2>
  friend bool operator==(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2);

  void inc_offset(difference_type bytes) noexcept {
    _ptr_or_offset += bytes;
    assert_not_overflow();
  }

  void dec_offset(difference_type bytes) noexcept {
    _ptr_or_offset -= bytes;
    assert_not_overflow();
  }

  void assert_not_overflow() {
    if constexpr (!std::is_same_v<value_type, void>) {
      if (_frame == nullptr) {
        return;
      }
      DebugAssert(_ptr_or_offset <= (bytes_for_size_type(_frame->size_type) + sizeof(PointedType)),
                  "BufferPtr overflow detected! " + std::to_string(static_cast<size_t>(_ptr_or_offset)) + " >" +
                      std::to_string(bytes_for_size_type(_frame->size_type) + sizeof(PointedType)));
    }
  }

  template <typename T>
  void unswizzle(T* ptr) {
    if (ptr) {
      const auto [frame, offset] = get_buffer_manager_memory_resource()->unswizzle(reinterpret_cast<const void*>(ptr));
      if (frame) {
        assert_not_overflow();
        _frame = frame;
        _ptr_or_offset = offset;
        return;
      } else {
        _frame = nullptr;
        _ptr_or_offset = reinterpret_cast<PtrOrOffset>(ptr);
        return;
      }
    } else {
      _frame = nullptr;
      _ptr_or_offset = 0;
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
  return ptr1.get_offset() - ptr2.get_offset();
}

template <class T1, class T2>
inline bool operator<=(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2) {
  return ptr1.get() <= ptr2.get();
}

template <class T>
inline void swap(BufferPtr<T>& ptr1, BufferPtr<T>& ptr2) {
  auto ptr1_frame = ptr1._frame;
  auto ptr1_offset = ptr1._ptr_or_offset;
  ptr1._frame = ptr2._frame;
  ptr1._ptr_or_offset = ptr2._ptr_or_offset;
  ptr2._frame = ptr1_frame;
  ptr2._ptr_or_offset = ptr1_offset;
}

}  // namespace hyrise
