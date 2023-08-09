#pragma once

namespace hyrise {

using DereferenceValue = std::string_view;

class VariableStringVectorIterator : public boost::iterator_facade<VariableStringVectorIterator, DereferenceValue,
                                                                   std::random_access_iterator_tag, DereferenceValue> {
 public:
  explicit VariableStringVectorIterator(const std::shared_ptr<const pmr_vector<char>>& dictionary,
                                        const std::shared_ptr<const pmr_vector<uint32_t>>& offset_vector,
                                        ValueID current_value_id)
      : _dictionary{dictionary}, _offset_vector{offset_vector}, _current_value_id{current_value_id} {}

 protected:
  friend class boost::iterator_core_access;

  // We have a couple of NOLINTs here becaues the facade expects these method names:

  bool equal(VariableStringVectorIterator const& other) const {  // NOLINT
    return _dictionary == other._dictionary && _current_value_id == other._current_value_id;
  }

  size_t distance_to(VariableStringVectorIterator const& other) const {  // NOLINT
    return static_cast<size_t>(other._current_value_id) - static_cast<size_t>(_current_value_id);
  }

  void advance(size_t n) {  // NOLINT
    _current_value_id += n;
  }

  void increment() {  // NOLINT
    ++_current_value_id;
  }

  void decrement() {  // NOLINT
    --_current_value_id;
  }

  const std::string_view dereference() const {  // NOLINT
    const auto offset = _offset_vector->operator[](_current_value_id);
    auto next_offset = 0;
    if (_current_value_id >= _offset_vector->size() - 1) {
      next_offset = _dictionary->size();
    } else {
      next_offset = _offset_vector->operator[](_current_value_id + 1);
    }
    const auto string_length = next_offset - offset - 1;
    return std::string_view{_dictionary->data() + offset, string_length};
  }

  std::shared_ptr<const pmr_vector<char>> _dictionary;
  ValueID _current_value_id;
  std::shared_ptr<const pmr_vector<uint32_t>> _offset_vector;
};

}  // namespace hyrise
