#pragma once

namespace hyrise {

class RowIdIterator {
 protected:
  std::vector<unsigned char>& _buffer;
  uint64_t _tuple_key_size;
  uint64_t _current_offset;

  RowIdIterator(std::vector<unsigned char>& buffer, uint64_t tuple_key_size, uint64_t current_offset);

 public:
  RowIdIterator(std::vector<unsigned char>& buffer, uint64_t tuple_key_size, bool is_end = false);
  RowIdIterator(const RowIdIterator& other);
  RowIdIterator& operator=(const RowIdIterator& other);
  RowIdIterator& operator++();    // prefix
  RowIdIterator operator++(int);  // postfix
  RowID operator*() const;
  const RowID* operator->() const;
  friend bool operator==(const RowIdIterator& lhs, const RowIdIterator& rhs);
  friend bool operator!=(const RowIdIterator& lhs, const RowIdIterator& rhs);
};

struct RowIdIteratorWithEnd {
  RowIdIterator iterator;
  RowIdIterator end;
};

}  // namespace hyrise