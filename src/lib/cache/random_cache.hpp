#pragma once

#include <list>
#include <random>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_cache_impl.hpp"

namespace opossum {

// Generic cache implementation using a random eviction policy.
// Note: This implementation is not thread-safe.
template <typename Key, typename Value>
class RandomCache : public AbstractCacheImpl<Key, Value> {
 public:
  using typename AbstractCacheImpl<Key, Value>::KeyValuePair;
  using typename AbstractCacheImpl<Key, Value>::AbstractIterator;
  using typename AbstractCacheImpl<Key, Value>::ErasedIterator;

  class Iterator : public AbstractIterator {
   public:
    using IteratorType = typename std::vector<KeyValuePair>::iterator;
    explicit Iterator(IteratorType p) : _wrapped_iterator(p) {}

   private:
    friend class boost::iterator_core_access;
    friend class AbstractCacheImpl<Key, Value>::ErasedIterator;

    IteratorType _wrapped_iterator;

    void increment() { ++_wrapped_iterator; }

    bool equal(const AbstractIterator& other) const {
      return _wrapped_iterator == static_cast<const Iterator&>(other)._wrapped_iterator;
    }

    const KeyValuePair& dereference() const { return *_wrapped_iterator; }
  };

  explicit RandomCache(size_t capacity)
      : AbstractCacheImpl<Key, Value>(capacity), _gen(_rd()), _rand(0, static_cast<int>(capacity - 1)) {
    _list.reserve(capacity);
  }

  // Sets the value to be cached at the given key.
  void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) {
    // Override old element at that key, if it exists.
    auto it = _map.find(key);
    if (it != _map.end()) {
      _list[it->second].second = value;
      return;
    }

    // If capacity is exceeded, pick a random element and replace it.
    if (_list.size() >= this->_capacity) {
      size_t index = _rand(_gen);
      _map.erase(_list[index].first);

      _list[index] = KeyValuePair(key, value);
      _map[key] = index;
      return;
    }

    // Otherwise simply add to the end of the vector.
    _list.push_back(KeyValuePair(key, value));
    _map[key] = _list.size() - 1;
  }

  // Retrieves the value cached at the key.
  Value& get(const Key& key) {
    auto it = _map.find(key);
    DebugAssert(it != _map.end(), "key not present");

    return _list[it->second].second;
  }

  bool has(const Key& key) const { return _map.find(key) != _map.end(); }

  size_t size() const { return _map.size(); }

  void clear() {
    _list.clear();
    _map.clear();
  }

  void resize(size_t capacity) {
    while (_list.size() > capacity) {
      _evict();
    }

    this->_capacity = capacity;
    _rand = std::uniform_int_distribution<>(0, static_cast<int>(capacity - 1));
  }

  ErasedIterator begin() { return ErasedIterator{std::make_unique<Iterator>(_list.begin())}; }

  ErasedIterator end() { return ErasedIterator{std::make_unique<Iterator>(_list.end())}; }

 protected:
  // List to hold all elements.
  std::vector<KeyValuePair> _list;

  // Map to point towards element in the list.
  std::unordered_map<Key, size_t> _map;

  // Random number generation to determine which item to evict.
  std::random_device _rd;
  std::mt19937 _gen;
  std::uniform_int_distribution<> _rand;

  void _evict() {
    _map.erase(_list[0].first);
    _list.erase(_list.cbegin());

    for (auto it = _map.cbegin(); it != _map.cend();) {
      _map[it->first] = it->second - 1;
      ++it;
    }
  }
};

}  // namespace opossum
