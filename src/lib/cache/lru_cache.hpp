#pragma once

#include <list>
#include <unordered_map>
#include <utility>

#include "abstract_cache_impl.hpp"

namespace opossum {

// Generic implementation of a least-recently-used cache.
// Note: This implementation is not thread-safe.
template <typename Key, typename Value>
class LRUCache : public AbstractCacheImpl<Key, Value> {
 public:
  using typename AbstractCacheImpl<Key, Value>::KeyValuePair;
  using typename AbstractCacheImpl<Key, Value>::AbstractIterator;
  using typename AbstractCacheImpl<Key, Value>::ErasedIterator;

  class LRUCacheIterator : public AbstractIterator {
   public:
    using list_iterator = typename std::list<KeyValuePair>::iterator;
    explicit LRUCacheIterator(list_iterator p) : _list_position(p) {}
    ~LRUCacheIterator() {}

   private:
    friend class boost::iterator_core_access;
    friend class AbstractCacheImpl<Key, Value>::ErasedIterator;

    list_iterator _list_position;

    void increment() { ++_list_position; }

    bool equal(AbstractIterator const& other) const {
      return _list_position == static_cast<const LRUCacheIterator&>(other)._list_position;
    }

    KeyValuePair& dereference() const { return *_list_position; }
  };

  explicit LRUCache(size_t capacity) : AbstractCacheImpl<Key, Value>(capacity) {}

  // Sets the value to be cached at the given key.
  void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) {
    _list.push_front(KeyValuePair(key, std::move(value)));

    // Override old element at that key, if it exists.
    auto it = _map.find(key);
    if (it != _map.end()) {
      _list.erase(it->second);
      _map.erase(it);
    }
    _map[key] = _list.begin();

    // Delete the last one, if capacity is exceeded.
    if (_map.size() > this->_capacity) {
      _evict();
    }
  }

  // Retrieves the value cached at the key.
  // Causes undefined behavior if the key is not in the cache.
  Value& get(const Key& key) {
    auto it = _map.find(key);
    _list.splice(_list.begin(), _list, it->second);
    return it->second->second;
  }

  bool has(const Key& key) const { return _map.find(key) != _map.end(); }

  // Returns the underlying list of all elements in the cache.
  std::list<KeyValuePair>& list() { return _list; }

  size_t size() const { return _map.size(); }

  void clear() {
    _list.clear();
    _map.clear();
  }

  void resize(size_t capacity) {
    while (_map.size() > capacity) {
      _evict();
    }
    this->_capacity = capacity;
  }

  ErasedIterator begin() {
    auto it = std::make_unique<LRUCacheIterator>(_list.begin());
    return ErasedIterator(std::move(it));
  }

  ErasedIterator end() {
    auto it = std::make_unique<LRUCacheIterator>(_list.end());
    return ErasedIterator(std::move(it));
  }

 protected:
  // Doubly-linked list to hold all elements.
  std::list<KeyValuePair> _list;

  // Map to point towards element in the list.
  std::unordered_map<Key, typename std::list<KeyValuePair>::iterator> _map;

  void _evict() {
    auto last = _list.end();
    last--;

    _map.erase(last->first);
    _list.pop_back();
  }
};

}  // namespace opossum
