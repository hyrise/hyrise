#pragma once

#include <utility>
#include <vector>

#include "abstract_pos_list.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// For a long time, PosList was just a pmr_vector<RowID>. With this class, we want to add functionality to that vector,
// more specifically, flags that give us some guarantees about its contents. If we know, e.g., that all entries point
// into the same chunk, we can simplify things in split_pos_list_by_chunk_id.
// Inheriting from std::vector is generally not encouraged, because the STL containers are not prepared for
// inheritance. By making the inheritance private and this class final, we can assure that the problems that come with
// a non-virtual destructor do not occur.

class RowIDPosList final : public AbstractPosList, private pmr_vector<RowID> {
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

  /* (1 ) */ RowIDPosList() noexcept(noexcept(allocator_type())) {}
  /* (1 ) */ explicit RowIDPosList(const allocator_type& allocator) noexcept : Vector(allocator) {}
  /* (2 ) */ RowIDPosList(size_type count, const RowID& value, const allocator_type& alloc = allocator_type())
      : Vector(count, value, alloc) {}
  /* (3 ) */ explicit RowIDPosList(size_type count, const allocator_type& alloc = allocator_type())
      : Vector(count, alloc) {}
  /* (4 ) */ template <class InputIt>
  RowIDPosList(InputIt first, InputIt last, const allocator_type& alloc = allocator_type())
      : Vector(std::move(first), std::move(last)) {}
  /* (5 ) */  // RowIDPosList(const Vector& other) : Vector(other); - Oh no, you don't.
  /* (5 ) */  // RowIDPosList(const Vector& other, const allocator_type& alloc) : Vector(other, alloc);
  /* (6 ) */ RowIDPosList(RowIDPosList&& other) noexcept
      : Vector(std::move(other)), _references_single_chunk{other._references_single_chunk} {}
  /* (6+) */ explicit RowIDPosList(Vector&& other) noexcept : Vector(std::move(other)) {}
  /* (7 ) */ RowIDPosList(RowIDPosList&& other, const allocator_type& alloc)
      : Vector(std::move(other), alloc), _references_single_chunk{other._references_single_chunk} {}
  /* (7+) */ RowIDPosList(Vector&& other, const allocator_type& alloc) : Vector(std::move(other), alloc) {}
  /* (8 ) */ RowIDPosList(std::initializer_list<RowID> init, const allocator_type& alloc = allocator_type())
      : Vector(std::move(init), alloc) {}

  RowIDPosList& operator=(RowIDPosList&& other) = default;

  // If we know that all entries in the RowIDPosList share a single ChunkID, we can optimize the indirection by
  // retrieving the respective chunk once and using only the ChunkOffsets to access values. As the consumer will likely
  // call table->get_chunk(common_chunk_id), we require that the common chunk id is valid, i.e., the RowIDPosList
  // contains no NULL values. Note that this is not about NULL values being referenced, but about NULL value contained
  // in the RowIDPosList itself.

  void guarantee_single_chunk();

  // Returns whether the single ChunkID has been given (not necessarily, if it has been met)
  bool references_single_chunk() const final;

  // For chunks that share a common ChunkID, returns that ID.
  ChunkID common_chunk_id() const final;

  using Vector::assign;
  using Vector::get_allocator;

  // Element access
  RowID operator[](size_t n) const final { return Vector::operator[](n); }

  RowID& operator[](size_t n) { return Vector::operator[](n); }

  using Vector::back;
  using Vector::data;
  using Vector::front;

  // Iterators
  using Vector::begin;
  using Vector::cbegin;
  using Vector::cend;
  using Vector::end;

  // Capacity
  using Vector::capacity;
  using Vector::max_size;
  using Vector::reserve;
  using Vector::shrink_to_fit;
  size_t size() const final { return Vector::size(); }
  bool empty() const final { return Vector::empty(); }

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

  size_t memory_usage(const MemoryUsageCalculationMode mode) const final;

 private:
  bool _references_single_chunk = false;
};

}  // namespace opossum
