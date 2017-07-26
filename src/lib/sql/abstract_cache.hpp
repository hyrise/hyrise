#pragma once

#include <utility>

namespace opossum {

// Generic template for a cache implementation.
template <typename key_t, typename val_t>
class AbstractCache {
 public:
  typedef typename std::pair<key_t, val_t> kv_pair_t;

  explicit AbstractCache(size_t capacity) : _capacity(capacity) {}

  virtual ~AbstractCache() {}

  // Cache the value at the given key.
  // If the new size exceeds the capacity an item may be evicted.
  // Depending on the underlying strategy, the parameters for cost and size may be used.
  // If they are not intended to be used, we specify constant default values here.
  virtual void set(key_t key, val_t value, double cost = 1.0, double size = 1.0) = 0;

  // Get the cached value at the given key.
  // Causes undefined behavior if the item is not in the cache.
  virtual val_t& get(key_t key) = 0;

  // Returns true if the cache holds an item at the given key.
  virtual bool has(key_t key) const = 0;

  // Returns the number of elements currently held in the cache.
  virtual size_t size() const = 0;

  // Remove all elements from the cache.
  virtual void clear() = 0;

  // Remove all elements from the cache and resize to the given capacity.
  virtual void clear_and_resize(size_t capacity) = 0;

  // Return the capacity of the cache.
  virtual size_t capacity() const { return _capacity; }

 protected:
  size_t _capacity;
};

}  // namespace opossum
