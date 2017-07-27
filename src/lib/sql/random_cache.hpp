#pragma once

#include <list>
#include <random>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_cache.hpp"

namespace opossum {

// Generic cache implementation using a random eviction policy.
// Note: This implementation is not thread-safe.
template <typename key_t, typename val_t>
class RandomCache : public AbstractCache<key_t, val_t> {
 public:
  typedef typename std::pair<key_t, val_t> kv_pair_t;

  explicit RandomCache(size_t capacity) : AbstractCache<key_t, val_t>(capacity), _gen(_rd()), _rand(0, capacity - 1) {
    _list.reserve(capacity);
  }

  // Sets the value to be cached at the given key.
  void set(const key_t& key, val_t value, double cost = 1.0, double size = 1.0) {
    // Override old element at that key, if it exists.
    auto it = _map.find(key);
    if (it != _map.end()) {
      it->second->second = value;
      return;
    }

    // If capacity is exceeded, pick a random element and replace it.
    if (_list.size() >= this->_capacity) {
      size_t index = _rand(_gen);
      _map.erase(_list[index].first);

      _list[index] = kv_pair_t(key, value);
      _map[key] = _list.begin() + index;
      return;
    }

    // Otherwise simply add to the end of the vector.
    _list.push_back(kv_pair_t(key, value));
    _map[key] = _list.begin() + (_list.size() - 1);
  }

  // Retrieves the value cached at the key.
  val_t& get(const key_t& key) {
    auto it = _map.find(key);
    return it->second->second;
  }

  bool has(const key_t& key) const { return _map.find(key) != _map.end(); }

  size_t size() const { return _map.size(); }

  void clear() {
    _list.clear();
    _map.clear();
  }

  void clear_and_resize(size_t capacity) {
    this->clear();
    this->_capacity = capacity;
    _list.shrink_to_fit();
    _list.reserve(capacity);
    _rand = std::uniform_int_distribution<>(0, capacity - 1);
  }

 protected:
  // List to hold all elements.
  std::vector<kv_pair_t> _list;

  // Map to point towards element in the list.
  std::unordered_map<key_t, typename std::vector<kv_pair_t>::iterator> _map;

  // Random number generation to determine which item to evict.
  std::random_device _rd;
  std::mt19937 _gen;
  std::uniform_int_distribution<> _rand;
};

}  // namespace opossum
