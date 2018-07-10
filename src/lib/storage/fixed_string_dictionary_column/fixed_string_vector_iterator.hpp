#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fixed_string.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// We need a custom iterator for this vector, since we have to perform jumps when iterating over the vector.
class FixedStringIterator
    : public boost::iterator_facade<FixedStringIterator, FixedString, std::random_access_iterator_tag, FixedString> {
 public:
  FixedStringIterator(size_t string_length, const pmr_vector<char>& vector, size_t pos);

  FixedStringIterator& operator=(const FixedStringIterator& other);

 private:
  using Facade = boost::iterator_facade<FixedStringIterator, FixedString, std::random_access_iterator_tag, FixedString>;
  friend class boost::iterator_core_access;

  // We have a couple of NOLINTs here becaues the facade expects these method names:

  bool equal(FixedStringIterator const& other) const;  // NOLINT

  typename Facade::difference_type distance_to(FixedStringIterator const& other) const;  // NOLINT

  void advance(typename Facade::difference_type n);  // NOLINT

  void increment();  // NOLINT

  void decrement();  // NOLINT

  FixedString dereference() const;  // NOLINT

  const size_t _string_length;
  const pmr_vector<char>& _chars;
  size_t _pos;
};

}  // namespace opossum
