#pragma once

#include <functional>
#include <utility>

#include "utils/assert.hpp"
#include "utils/bit_utils.hpp"

namespace opossum {

template<typename Value>
class StaticHashMapIterator {
 public:
  StaticHashMapIterator(Value* value):
    _value(value)
  {}

  Value& operator*() {
    return *_value;
  }

  Value* operator->() {
    return _value;
  }

  friend bool operator==(const StaticHashMapIterator<Value>& lhs, const StaticHashMapIterator<Value>& rhs) {
    return lhs._value == rhs._value;
  }

  friend bool operator!=(const StaticHashMapIterator<Value>& lhs, const StaticHashMapIterator<Value>& rhs) {
    return lhs._value != rhs._value;
  }

 private:
  Value* _value;
};

template<typename Value>
bool operator==(const StaticHashMapIterator<Value>& lhs, const StaticHashMapIterator<Value>& rhs) {
  return lhs._value == rhs._value;
}

template<typename Value>
bool operator!=(const StaticHashMapIterator<Value>& lhs, const StaticHashMapIterator<Value>& rhs) {
  return lhs._value != rhs._value;
}

template<typename Key, typename T, typename Hash = std::hash<Key>, typename KeyCompare = std::equal_to<Key>>
class StaticHashMap {
 public:
  using value_type = std::pair<Key, T>;
  using iterator = StaticHashMapIterator<value_type>;

  explicit StaticHashMap(const size_t bucket_count, const Hash& hash = {}, const KeyCompare& key_compare = {}):
    _slot_occupied(bucket_count, false),
    _table(bucket_count, value_type{}),
    _mask(bucket_count - 1), _hash(hash), _key_compare(key_compare) {
    DebugAssert(is_power_of_two(bucket_count), "Bucket count has to be power of two");
  }

  iterator find(const Key& key) {
    for (auto slot_idx = _hash(key) & _mask; ; slot_idx = (slot_idx + 1) & _mask) {
      if (!_slot_occupied[slot_idx]) {
        return end();
      } else if (_key_compare(_table[slot_idx].first, key)) {
        return &_table[slot_idx];
      }
    }
  }

  std::pair<iterator, bool> insert(value_type&& value) {
    DebugAssert(_count < _table.size(), "Hash table is full");

    for (auto slot_idx = _hash(value.first) & _mask; ; slot_idx = (slot_idx + 1) & _mask) {
      if (!_slot_occupied[slot_idx]) {
        _table[slot_idx] = std::move(value);
        _slot_occupied[slot_idx] = true;
        ++_count;

        return {&_table[slot_idx], true};
      } else if (_key_compare(_table[slot_idx].first, value.first)) {
        Fail("Cannot insert into occupied slot");
      }
    }
  }

  iterator end() {
    return _table.data() + _table.size();
  }

  float load_factor() const {
    return static_cast<float>(_count) / _table.size();
  }

  size_t size() const {
    return _count;
  }

  Hash hash_function() const {
    return _hash;
  }

  KeyCompare key_eq() const {
    return _key_compare;
  }

 private:
  size_t _get_slot_idx(const Key& key) const {
    for (auto slot_idx = _hash(key) & _mask; ; slot_idx = (slot_idx + 1) & _mask) {
      if (!_slot_occupied[slot_idx]) {
        return slot_idx;
      } else if (_key_compare(_table[slot_idx].first, key)) {
        return slot_idx;
      }
    }
  }

  std::vector<bool> _slot_occupied;
  std::vector<value_type> _table;
  size_t _count{0};
  size_t _mask{0};
  Hash _hash;
  KeyCompare _key_compare;
};

}  // namespace opossum
