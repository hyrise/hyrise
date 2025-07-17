#pragma once

namespace hyrise {

class RowIdIterator : public std::iterator<std::input_iterator_tag, const RowID> {
 protected:
  const std::vector<unsigned char>& _buffer;
  uint64_t _tuple_key_size;
  uint64_t _current_offset;

 public:
  RowIdIterator(const std::vector<unsigned char>& buffer, uint64_t tuple_key_size);
  RowIdIterator(const std::vector<unsigned char>& buffer, uint64_t tuple_key_size, uint64_t current_offset);
  RowIdIterator& operator++();    // prefix
  RowIdIterator operator++(int);  // postfix
  value_type operator*() const;
  pointer operator->() const;
  friend bool operator==(const RowIdIterator& lhs, const RowIdIterator& rhs);
  friend bool operator!=(const RowIdIterator& lhs, const RowIdIterator& rhs);
};

struct RowIdIteratorWithEnd {
  RowIdIterator iterator;
  RowIdIterator end;
};

}  // namespace hyrise