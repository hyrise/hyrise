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
      BufferManager::get_global_buffer_manager().pin(_frame);
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
    const auto dirty = _access_intent == AccessIntent::Write;
    BufferManager::get_global_buffer_manager().unpin(_frame, dirty);
  }

 private:
  std::shared_ptr<Frame> _frame;
  const AccessIntent _access_intent;
};

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

  BufferPtr(const BufferPtr& other) : _shared_frame(other._shared_frame), _ptr_or_offset(other._ptr_or_offset) {
    DebugAssert(!_shared_frame || _ptr_or_offset < bytes_for_size_type(_shared_frame->dram_frame->size_type),
                "BufferPtr overflow BufferPtr(const BufferPtr& other)");
  }

  template <typename U>
  BufferPtr(const BufferPtr<U>& other) : _shared_frame(other._shared_frame), _ptr_or_offset(other._ptr_or_offset) {
    DebugAssert(!_shared_frame || _ptr_or_offset < bytes_for_size_type(_shared_frame->dram_frame->size_type),
                "BufferPtr overflow BufferPtr(const BufferPtr<U>& other)");
  }

  template <typename T>
  BufferPtr(T* ptr) {
    unswizzle(ptr);
  }

  explicit BufferPtr(const std::shared_ptr<SharedFrame> frame, const PtrOrOffset ptr_or_offset)
      : _shared_frame(frame), _ptr_or_offset(ptr_or_offset) {
    DebugAssert(!_shared_frame || _ptr_or_offset < bytes_for_size_type(_shared_frame->dram_frame->size_type),
                "BufferPtr overflow BufferPtr(const std::shared_ptr<SharedFrame> frame, const PtrOrOffset");
  }

  pointer operator->() const {
    return get();
  }

  reference operator*() const {
    pointer p = this->get();
    reference r = *p;
    return r;
  }

  reference operator[](std::ptrdiff_t idx) {
    return get(AccessIntent::Write)[idx];
  }

  const reference operator[](std::ptrdiff_t idx) const {
    return get(AccessIntent::Read)[idx];
  }

  bool operator!() const {
    return !_shared_frame && !_ptr_or_offset;
  }

  BufferPtr operator+(difference_type offset) const noexcept {
    auto new_ptr = BufferPtr(_shared_frame, _ptr_or_offset);
    new_ptr.inc_offset(offset * difference_type(sizeof(PointedType)));
    return new_ptr;
  }

  BufferPtr operator-(difference_type offset) const noexcept {
    auto new_ptr = BufferPtr(_shared_frame, _ptr_or_offset);
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
    _shared_frame = ptr._shared_frame;
    _ptr_or_offset = ptr._ptr_or_offset;
    return *this;
  }

  BufferPtr& operator=(pointer from) {
    unswizzle(from);
    return *this;
  }

  template <typename T2>
  BufferPtr& operator=(const BufferPtr<T2>& ptr) {
    _shared_frame = ptr._shared_frame;
    _ptr_or_offset = ptr._ptr_or_offset;
    return *this;
  }

  explicit operator bool() const noexcept {
    // TODO: Load frame
    return _shared_frame || _ptr_or_offset;
  }

  pointer get(const AccessIntent access_intent = AccessIntent::Read) const {
    return static_cast<pointer>(this->get_pointer(access_intent));
  }

  static BufferPtr pointer_to(reference r) {
    return BufferPtr(&r);
  }

  friend bool operator==(const BufferPtr& ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1._shared_frame == ptr2._shared_frame && ptr1._ptr_or_offset == ptr2._ptr_or_offset;
  }

  friend bool operator==(pointer ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1 == ptr2.get();
  }

  BufferPtr& operator++(void) noexcept {
    inc_offset(difference_type(sizeof(PointedType)));
    return *this;
  }

  BufferPtr operator++(int) noexcept {
    const auto temp = BufferPtr(_shared_frame, _ptr_or_offset);
    inc_offset(difference_type(sizeof(PointedType)));
    return temp;
  }

  BufferPtr& operator--(void) noexcept {
    dec_offset(difference_type(sizeof(PointedType)));
    return *this;
  }

  friend bool operator<(const BufferPtr& ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1._shared_frame == ptr2._shared_frame && ptr1._ptr_or_offset < ptr2._ptr_or_offset;
  }

  friend bool operator<(pointer& ptr1, const BufferPtr& ptr2) noexcept {
    return ptr1 < ptr2.get();
  }

  friend bool operator<(const BufferPtr& ptr1, pointer ptr2) noexcept {
    return ptr1.get() < ptr2;
  }

  void* get_pointer(const AccessIntent access_intent) const {
    if (_shared_frame) {
      const auto frame = BufferManager::get_global_buffer_manager().load_frame(_shared_frame, access_intent);
      return frame->data + _ptr_or_offset;
    } else {
      return (void*)_ptr_or_offset;
    }
  }

  pointer pin(FramePinGuard& guard) const {
    auto frame = guard.get_frame();
    if (!frame) {
      frame = BufferManager::get_global_buffer_manager().load_frame(_shared_frame, guard.get_access_intent());
      guard.pin(frame);
    }
    return reinterpret_cast<pointer>(frame->data + _ptr_or_offset);
  }

  std::shared_ptr<Frame> get_frame(const AccessIntent access_intent) const {
    return BufferManager::get_global_buffer_manager().load_frame(_shared_frame, access_intent);
  }

  PtrOrOffset get_offset() const {
    return _ptr_or_offset;
  }

  std::shared_ptr<SharedFrame> get_shared_frame() const {
    return _shared_frame;
  }

 private:
  std::shared_ptr<SharedFrame> _shared_frame;
  PtrOrOffset _ptr_or_offset = 0;

  template <class T1, class T2>
  friend bool operator!=(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2);

  template <class T1, class T2>
  friend bool operator==(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2);

  void inc_offset(difference_type bytes) noexcept {
    DebugAssert(!_shared_frame || _ptr_or_offset + bytes <= bytes_for_size_type(_shared_frame->dram_frame->size_type),
                "BufferPtr overflow inc_offset");
    _ptr_or_offset += bytes;
  }

  void dec_offset(difference_type bytes) noexcept {
    DebugAssert(!_shared_frame || _ptr_or_offset - bytes > 0, "BufferPtr overflow dec_offset");
    _ptr_or_offset -= bytes;
  }

  template <typename T>
  void unswizzle(T* ptr) {
    if (ptr) {
      const auto [frame, offset] =
          BufferManager::get_global_buffer_manager().unswizzle(reinterpret_cast<const void*>(ptr));
      if (frame) {
        DebugAssert(offset <= bytes_for_size_type(frame->size_type), "BufferPtr overflow unswizzle");
        _shared_frame = std::make_shared<SharedFrame>(frame);
        _ptr_or_offset = offset;
        return;
      } else {
        _shared_frame = nullptr;
        _ptr_or_offset = reinterpret_cast<PtrOrOffset>(ptr);
        return;
      }
    } else {
      _shared_frame = nullptr;
      _ptr_or_offset = 0;
    }
  }
};

template <class T1, class T2>
inline bool operator==(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2) {
  return ptr1.get_shared_frame() == ptr2.get_shared_frame() && ptr1._ptr_or_offset == ptr2._ptr_or_offset;
}

template <class T1, class T2>
inline bool operator!=(const BufferPtr<T1>& ptr1, const BufferPtr<T2>& ptr2) {
  return ptr1.get_shared_frame() != ptr2.get_shared_frame() || ptr1._ptr_or_offset != ptr2._ptr_or_offset;
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
  return ptr1.get_shared_frame() == ptr2.get_shared_frame() && ptr1.get_offset() <= ptr2.get_offset();
}

template <class T>
inline void swap(BufferPtr<T>& ptr1, BufferPtr<T>& ptr2) {
  auto ptr1_frame = ptr1._shared_frame;
  auto ptr1_offset = ptr1._ptr_or_offset;
  ptr1._shared_frame = ptr2._shared_frame;
  ptr1._ptr_or_offset = ptr2._ptr_or_offset;
  ptr2._shared_frame = ptr1_frame;
  ptr2._ptr_or_offset = ptr1_offset;
}

}  // namespace hyrise

namespace std {
template <class T>
struct hash<hyrise::BufferPtr<T>> {
  size_t operator()(const hyrise::BufferPtr<T>& ptr) const noexcept {
    // TODO
    return std::hash<typename hyrise::BufferPtr<T>>(ptr._addressing);
  }
};
}  // namespace std