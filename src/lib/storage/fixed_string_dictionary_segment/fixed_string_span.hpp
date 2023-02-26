#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fixed_string.hpp"
#include "fixed_string_vector.hpp"
#include "fixed_string_span_iterator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// FixedStringSpan is a data type, which stores all its values in a vector and
// is capable of storing FixedStrings.

class FixedStringSpan {
 public:
  FixedStringSpan(const FixedStringVector& vector);
  FixedStringSpan(const char* start_address, const uint32_t string_length, const uint32_t size);

  // Return the value at a certain position.
  // FixedString operator[](const size_t pos);

  pmr_string get_string_at(const size_t pos) const;

  // Make the FixedStringSpan of FixedStrings iterable in different ways
  FixedStringSpanIterator<false> begin() noexcept;
  FixedStringSpanIterator<false> end() noexcept;
  FixedStringSpanIterator<true> begin() const noexcept;
  FixedStringSpanIterator<true> end() const noexcept;
  FixedStringSpanIterator<true> cbegin() const noexcept;
  FixedStringSpanIterator<true> cend() const noexcept;

  using ReverseIterator = boost::reverse_iterator<FixedStringSpanIterator<false>>;
  ReverseIterator rbegin() noexcept;
  ReverseIterator rend() noexcept;

  // Return a pointer to the underlying memory
  const char* data() const;

  // Return the number of entries in the vector.
  size_t size() const;

  size_t string_length() const;

  // // Return the calculated size of FixedStringSpan in main memory
  // size_t data_size() const;

 protected:
  const size_t _string_length;
  std::span<const char> _chars;
  size_t _size = 0;
};

}  // namespace hyrise
