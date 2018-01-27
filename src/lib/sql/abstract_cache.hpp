#pragma once

#include <utility>

namespace opossum {

// Generic template for a cache implementation.
template <typename Key, typename Value>
class AbstractCache {
 public:
  typedef typename std::pair<Key, Value> KeyValuePair;

  explicit AbstractCache(size_t capacity) : _capacity(capacity) {}

  virtual ~AbstractCache() {}

  // Cache the value at the given key.
  // If the new size exceeds the capacity an item will be evicted.
  // Depending on the underlying strategy, the parameters for cost and size may be used.
  // If they are not intended to be used, we specify constant default values here.
  virtual void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) = 0;

  // Get the cached value at the given key.
  // Causes undefined behavior if the item is not in the cache.
  virtual Value& get(const Key& key) = 0;

  // Returns true if the cache holds an item at the given key.
  virtual bool has(const Key& key) const = 0;

  // Returns the number of elements currently held in the cache.
  virtual size_t size() const = 0;

  // Remove all elements from the cache.
  virtual void clear() = 0;

  // Resize to the given capacity.
  virtual void resize(size_t capacity) = 0;

  // Return the capacity of the cache.
  size_t capacity() const { return _capacity; }

 protected:
  // Remove an element from the cache according to the cache algorithm's strategy
  virtual void _evict() = 0;

  size_t _capacity;
};

}  // namespace opossum
