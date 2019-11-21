#pragma once

#include <utility>

#include <boost/iterator/iterator_facade.hpp>

namespace opossum {

// Generic template for a cache implementation.
template <typename Key, typename Value>
class AbstractCacheImpl {
 public:
  using KeyValuePair = typename std::pair<Key, Value>;

  struct AbstractIterator {
    virtual ~AbstractIterator() = default;

    virtual void increment() = 0;
    virtual bool equal(const AbstractIterator& other) const = 0;
    virtual const KeyValuePair& dereference() const = 0;
  };

  class ErasedIterator
      : public boost::iterator_facade<ErasedIterator, KeyValuePair const, boost::forward_traversal_tag> {
   public:
    explicit ErasedIterator(std::unique_ptr<AbstractIterator> it) : _it(std::move(it)) {}

   private:
    friend class boost::iterator_core_access;

    void increment() { _it->increment(); }
    bool equal(const ErasedIterator& other) const { return _it->equal(*other._it); }
    const KeyValuePair& dereference() const { return _it->dereference(); }

    std::unique_ptr<AbstractIterator> _it;
  };

  explicit AbstractCacheImpl(size_t capacity) : _capacity(capacity) {}

  virtual ~AbstractCacheImpl() {}

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

  virtual ErasedIterator begin() = 0;
  virtual ErasedIterator end() = 0;

  // Return the capacity of the cache.
  size_t capacity() const { return _capacity; }

 protected:
  // Remove an element from the cache according to the cache algorithm's strategy
  virtual void _evict() = 0;

  size_t _capacity;
};

}  // namespace opossum
