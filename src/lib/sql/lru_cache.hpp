#pragma once

#include <list>
#include <unordered_map>
#include <utility>

namespace opossum {

// Generic implementation of a least-recently-used cache.
// Note: This implemenation is not thread-safe.
template <typename key_t, typename val_t>
class LRUCache {
 public:
  typedef typename std::pair<key_t, val_t> kv_pair_t;

  explicit LRUCache(size_t capacity) : _capacity(capacity) {}

  // Sets the value to be cached at the given key.
  inline void set(key_t key, val_t value) {
    _list.push_front(kv_pair_t(key, std::move(value)));

    // Override old element at that key, if it exists.
    auto it = _map.find(key);
    if (it != _map.end()) {
      _list.erase(it->second);
      _map.erase(it);
    }
    _map[key] = _list.begin();

    // Delete the last one, if capacity is exceeded.
    if (_map.size() > _capacity) {
      auto last = _list.end();
      last--;

      _map.erase(last->first);
      _list.pop_back();
    }
  }

  // Retrieves the value cached at the key.
  // Causes undefined behavior if the key is not in the cache.
  inline val_t get(key_t key) {
    auto it = _map.find(key);
    _list.splice(_list.begin(), _list, it->second);
    return it->second->second;
  }

  const inline bool has(key_t key) const { return _map.find(key) != _map.end(); }

  // Returns the underlying list of all elements in the cache.
  std::list<kv_pair_t>& list() { return _list; }

  size_t size() const { return _map.size(); }

  // Return the capacity of the cache.
  size_t capacity() const { return _capacity; }

 private:
  // Doubly-linked list to hold all elements.
  std::list<kv_pair_t> _list;

  // Map to point towards element in the list.
  std::unordered_map<key_t, typename std::list<kv_pair_t>::iterator> _map;

  size_t _capacity;
};

}  // namespace opossum
