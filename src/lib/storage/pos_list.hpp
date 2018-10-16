#pragma once

#include <utility>
#include <vector>

#include "types.hpp"

namespace opossum {

// TODO explain why this has to be final

struct PosList : private pmr_vector<RowID> {
 public:
  using Vector = pmr_vector<RowID>;

  using value_type = Vector::value_type;
  using allocator_type = Vector::allocator_type;
  using size_type = Vector::size_type;
  using difference_type = Vector::difference_type;
  using reference = Vector::reference;
  using const_reference = Vector::const_reference;
  using pointer = Vector::pointer;
  using const_pointer = Vector::const_pointer;
  using iterator = Vector::iterator;
  using const_iterator = Vector::const_iterator;
  using reverse_iterator = Vector::reverse_iterator;
  using const_reverse_iterator = Vector::const_reverse_iterator;

  /* (1 ) */ PosList() noexcept(noexcept(allocator_type())) {}
  /* (1 ) */ explicit PosList(const allocator_type& allocator) noexcept : Vector(allocator) {}
  /* (2 ) */ PosList(size_type count, const RowID& value, const allocator_type& alloc = allocator_type())
      : Vector(count, value, alloc) {}
  /* (3 ) */ explicit PosList(size_type count, const allocator_type& alloc = allocator_type()) : Vector(count, alloc) {}
  /* (4 ) */ template <class InputIt>
  PosList(InputIt first, InputIt last, const allocator_type& alloc = allocator_type())
      : Vector(std::move(first), std::move(last)) {}
  /* (5 ) */  // PosList(const Vector& other) : Vector(other); - Oh no, you don't.
  /* (5 ) */  // PosList(const Vector& other, const allocator_type& alloc) : Vector(other, alloc);
  /* (6 ) */ PosList(PosList&& other) noexcept : Vector(std::move(other)) {}
  /* (6+) */ PosList(Vector&& other) noexcept : Vector(std::move(other)) {}
  /* (7 ) */ PosList(PosList&& other, const allocator_type& alloc) : Vector(std::move(other), alloc) {}
  /* (7+) */ PosList(Vector&& other, const allocator_type& alloc) : Vector(std::move(other), alloc) {}
  /* (8 ) */ PosList(std::initializer_list<RowID> init, const allocator_type& alloc = allocator_type())
      : Vector(std::move(init), alloc) {}

  PosList& operator=(PosList&& other) {
    Vector::operator=(std::move(other));
    return *this;
  }
  void guarantee_single_chunk() { _references_single_chunk = true; }

  bool references_single_chunk() const { return _references_single_chunk; }

  std::vector<ChunkOffset> convert_to_offsets() const {
    Assert(references_single_chunk(),
           "Can only convert PosList to ChunkOffsetsList if it is guaranteed to refer to a single chunk");
    std::vector<ChunkOffset> list(size());
    for (auto i = 0ul; i < size(); ++i) list[i] = (*this)[i].chunk_offset;
    return list;
  }

  using Vector::assign;
  using Vector::get_allocator;

  // Element access
  // using Vector::at; - Oh no. People have misused this in the past.
  using Vector::operator[];
  using Vector::back;
  using Vector::data;
  using Vector::front;

  // Iterators
  using Vector::begin;
  using Vector::cbegin;
  using Vector::cend;
  using Vector::crbegin;
  using Vector::crend;
  using Vector::end;
  using Vector::rbegin;
  using Vector::rend;

  // Capacity
  using Vector::capacity;
  using Vector::empty;
  using Vector::max_size;
  using Vector::reserve;
  using Vector::shrink_to_fit;
  using Vector::size;

  // Modifiers
  using Vector::clear;
  using Vector::emplace;
  using Vector::emplace_back;
  using Vector::erase;
  using Vector::insert;
  using Vector::pop_back;
  using Vector::push_back;
  using Vector::resize;
  using Vector::swap;

  friend bool operator==(const PosList& lhs, const PosList& rhs);
  friend bool operator==(const PosList& lhs, const pmr_vector<RowID>& rhs);
  friend bool operator==(const pmr_vector<RowID>& lhs, const PosList& rhs);

 private:
  bool _references_single_chunk = false;
};

inline bool operator==(const PosList& lhs, const PosList& rhs) {
  return static_cast<pmr_vector<RowID>>(lhs) == static_cast<pmr_vector<RowID>>(rhs);
}

inline bool operator==(const PosList& lhs, const pmr_vector<RowID>& rhs) {
  return static_cast<pmr_vector<RowID>>(lhs) == rhs;
}

inline bool operator==(const pmr_vector<RowID>& lhs, const PosList& rhs) {
  return lhs == static_cast<pmr_vector<RowID>>(rhs);
}

}  // namespace opossum
