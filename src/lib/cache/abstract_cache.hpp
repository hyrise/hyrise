#pragma once

#include <atomic>
#include <optional>
#include <unordered_map>

namespace opossum {

inline constexpr size_t DEFAULT_CACHE_CAPACITY = 1024;

// Generic template for a cache implementation.
// It guarantees the capacity to be thread-safe. Any other guarantees have to be fulfilled by the implementation.
template <typename Key, typename Value>
class AbstractCache {
 public:
  explicit AbstractCache(size_t capacity = DEFAULT_CACHE_CAPACITY) : _capacity(capacity) {}

  virtual ~AbstractCache() {}

  // Cache the value at the given key.
  // If the new size exceeds the capacity an item will be evicted.
  // Depending on the underlying strategy, the parameters for cost and size may be used.
  // If they are not intended to be used, we specify constant default values here.
  virtual void set(const Key& key, const Value& value, double cost = 1.0, double size = 1.0) = 0;

  // Tries to fetch a cache entry. We cannot return a reference since this could not be thread-safe.
  virtual std::optional<Value> try_get(const Key& key) = 0;

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

  struct SnapshotEntry {
    Value value;
    std::optional<size_t> frequency;
  };

  // Provide a copy of the entries to iterate over them in a thread-safe way
  virtual std::unordered_map<Key, SnapshotEntry> snapshot() const = 0;

 protected:
  // Remove an element from the cache according to the cache algorithm's strategy
  virtual void _evict() = 0;

  std::atomic_size_t _capacity;
};

}  // namespace opossum
