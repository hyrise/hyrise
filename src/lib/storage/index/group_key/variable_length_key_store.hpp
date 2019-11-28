#pragma once

#include <memory>
#include <ostream>
#include <vector>

#include <boost/iterator/iterator_facade.hpp>

#include "variable_length_key.hpp"

namespace opossum {

class VariableLengthKeyProxy;
class VariableLengthKeyConstProxy;

/**
 * This class stores multiple VariableLengthKeys with a specified byte length of each entry.
 * The intention is to use this class as replacement for std::vector<VariableLengthKey> if every stored
 * entry has the specified byte size. The provided advantage is the continuous placement of the key data in
 * memory. Since every key has to manage memory by itself in order to achieve variable lengths, a std::vector
 * can only store the VariableKeyLength objects continuously in memory, every object pointing to its memory location.
 * The VariableLengthKeyStore also provides continuous placement of the data pointed to by VariableLengthKey objects.
 * Additionally, every entry is saved word-aligned.
 *
 * In order to achieve the desired result, a proxied container is implemented. The implementation is similiar to
 * common std::vector<bool> implementations, sharing the same drawbacks: a proxied container can never fulfill
 * the requirements of a Container and provided iterators can never be standard conform RandomAccessIterators.
 * This implies all common algorithms provided by the standard library are not guaranteed to work with these, but
 * clang and gcc seem to work properly.
 *
 * Usage hint: if you want to iterate over this container with a range based for-loop, use universal references
 * or a copy instead of a reference:
 *   for(auto && v : values) {...} with values as an instance of VariableLengthKeyStore.
 *   for(auto v : values) {...} with values as an instance of VariableLengthKeyStore.
 *
 */
class VariableLengthKeyStore {
 private:
  template <typename T>
  class IteratorBase;

 public:
  using const_iterator = IteratorBase<VariableLengthKeyConstProxy>;
  using iterator = IteratorBase<VariableLengthKeyProxy>;

  /**
   * Creates a new instance of VariableLengthKeyStore, without space for entries and a required key size
   * of zero bytes per key, resulting in an unusable instance.
   */
  VariableLengthKeyStore() = default;

  /**
   * Creates a new instance of VariableLengthKeyStore, with space for size entries and a required size bytes_per_key for
   * each entry.
   */
  explicit VariableLengthKeyStore(ChunkOffset size, CompositeKeyLength bytes_per_key);

  /**
   * Mimics the operator[] as used in std::vector with the difference that proxy objects are returned instead of
   * references. Although proxy objects are returned, they can be nearly used as if they would be references to
   * VariableLengthKeys. The only limitation is that for writing the length of the new key has to match the size of the
   * already stored key.
   */
  VariableLengthKeyProxy operator[](ChunkOffset position);
  VariableLengthKeyConstProxy operator[](ChunkOffset position) const;

  /**
   * Returns how many bytes are at least required to save a single key. The returned size does not have to match the
   * actual size used for saving the keys internally, since the keys are aligned on word borders.
   */
  CompositeKeyLength key_size() const;

  /**
   * Returns how many entries are stored in the container.
   */
  ChunkOffset size() const;

  /**
   * Resizes the container to the specified size. If size is smaller than size(), entries are deleted. If size is
   * larger,
   * default entries are created, which are all zero.
   */
  void resize(ChunkOffset size);

  /**
   * Returns unused space back to the operating system.
   */
  void shrink_to_fit();

  /**
   * Removes elements in range [first; last).
   */
  iterator erase(iterator first, iterator last);

  iterator begin();
  iterator end();
  const_iterator begin() const;
  const_iterator end() const;
  const_iterator cbegin() const;
  const_iterator cend() const;

 private:
  CompositeKeyLength _bytes_per_key;
  CompositeKeyLength _key_alignment;
  std::vector<VariableLengthKeyWord> _data;

 private:
  /**
   * Implementation for iterator and const_iterator using boost::iterator_facade. The template is used in order to
   * reduce
   * code duplication.
   */
  template <typename Proxy>
  class IteratorBase : public boost::iterator_facade<IteratorBase<Proxy>, VariableLengthKey,
                                                     std::random_access_iterator_tag, Proxy, std::ptrdiff_t> {
    friend class VariableLengthKeyStore;
    friend class boost::iterator_core_access;

   public:
    IteratorBase() = default;

    /**
     * Creates new instance copying the other iterator iff the other proxy type is convertible into the own type.
     * This is required since a mutable constructor can be used every time when a const iterator is expected.
     */
    template <typename OtherProxy, typename = std::enable_if_t<std::is_convertible_v<OtherProxy, Proxy>>>
    IteratorBase(const IteratorBase<OtherProxy>& other)  // NOLINT(runtime/explicit)
        : _bytes_per_key(other._bytes_per_key), _key_alignment(other._key_alignment), _data(other._data) {}

   private:
    /**
     * Creates new iterator pointing to given address.
     */
    explicit IteratorBase(CompositeKeyLength bytes_per_key, CompositeKeyLength key_alignment,
                          VariableLengthKeyWord* data)
        : _bytes_per_key(bytes_per_key), _key_alignment(key_alignment), _data(data) {}

    void increment() { _data += _key_alignment; }
    void decrement() { _data -= _key_alignment; }
    void advance(ChunkOffset n) { _data += n * _key_alignment; }

    Proxy dereference() const { return Proxy(_data, _bytes_per_key); }

    /**
     * Check for equality between any combination of iterator and const_iterator
     */
    template <typename OtherProxy>
    bool equal(const IteratorBase<OtherProxy>& other) const {
      return _data == other._data && _bytes_per_key == other._bytes_per_key;
    }

    /**
     * Calculate distance between any combination of iterator and const_iterator
     */
    template <typename OtherProxy>
    std::ptrdiff_t distance_to(const IteratorBase<OtherProxy>& other) const {
      return (other._data - _data) / _key_alignment;
    }

   private:
    CompositeKeyLength _bytes_per_key;
    CompositeKeyLength _key_alignment;
    VariableLengthKeyWord* _data;
  };
};

}  // namespace opossum
