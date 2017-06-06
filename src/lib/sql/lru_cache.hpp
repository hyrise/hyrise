#pragma once

#include <list>
#include <map>

namespace opossum {

template <typename key_t, typename val_t>
class LRUCache {
 public:
  typedef typename std::pair<key_t, val_t> kv_pair_t;

  LRUCache(size_t capacity) : _capacity(capacity) {}

  // Sets the value to be cached at the given key.
  inline void set(key_t key, val_t value) {
    auto it = _map.find(key);
    _list.push_front(kv_pair_t(key, std::move(value)));

    // Override old element at that key.
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

  // Retrieves the value cached at the key into *value.
  // Returns false if new value could be returned.
  inline val_t get(key_t key) {
    auto it = _map.find(key);
    _list.splice(_list.begin(), _list, it->second);
    return it->second->second;
  }

  const inline bool has(key_t key) const {
    auto it = _map.find(key);
    return !(it == _map.end());
  }

  std::list<kv_pair_t>& list() { return _list; }

  size_t capacity() const { return _capacity; }

 private:
  // Doubly-linked list to hold all elements.
  std::list<kv_pair_t> _list;

  // Map to point towards element in the list.
  std::map<key_t, typename std::list<kv_pair_t>::iterator> _map;

  size_t _capacity;
};

}  // namespace opossum
