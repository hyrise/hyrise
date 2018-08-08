#pragma once

#include <boost/variant.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "murmur_hash.hpp"
#include "type_comparison.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/*
Insert-only implementation of Cuckoo Hash Table. The HashTable is currently only used for HashJoins,
where it is a temporary object for probing. There is no need to delete elements in that use case.
*/
template <typename T>
class HashTable : private Noncopyable {
  static const size_t NUMBER_OF_HASH_FUNCTIONS = 3;

 public:
  explicit HashTable(size_t input_table_size) : _input_table_size(input_table_size) {
    // prepare internal hash tables and fill with empty elements
    // can't use resize because elements are not copyable
    for (size_t i = 0; i < NUMBER_OF_HASH_FUNCTIONS; ++i) {
      _hashtables.emplace_back(input_table_size);
    }
  }

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  HashTable(HashTable&&) = default;
  HashTable& operator=(HashTable&&) = default;

  /*
  Insert a new element into hashtable
  */
  void put(const T value, const RowID row_id) {
    // Check whether value is already in hashtable, then just add row id
    for (size_t i = 0; i < NUMBER_OF_HASH_FUNCTIONS; i++) {
      auto position = hash<T>(i, value);
      auto& element = _hashtables[i][position];
      if (element && value_equal(element->value, value)) {
        if (element->row_ids.type() == typeid(RowID)) {
          // Previously, there was only one row id stored for this value. Convert the entry to a multi-row-id one.
          element->row_ids = PosList{boost::get<RowID>(element->row_ids), row_id};
        } else {
          boost::get<PosList>(element->row_ids).push_back(row_id);
        }
        return;
      }
    }
    auto element = HashElement{value, row_id};
    place(std::move(element), 0, 0);
  }

  /*
  Retrieve an element from hashtable returning true or false indicating the success.
  All the matching RowIDs are returned in row_ids.
  */
  template <typename S>
  std::optional<std::reference_wrapper<const boost::variant<RowID, PosList>>> get(S value) const {
    for (size_t i = 0; i < NUMBER_OF_HASH_FUNCTIONS; i++) {
      auto position = hash<S>(i, value);
      const auto& element = _hashtables[i][position];
      if (element && value_equal(element->value, value)) {
        return std::cref(element->row_ids);
      }
    }
    return std::nullopt;
  }

 protected:
  /*
  We use this struct internally for storing data. It should not be exposed to other classes.
  */
  struct HashElement : private Noncopyable {
    HashElement(T v, RowID r) : value(v), row_ids(r) {}
    HashElement(T v, PosList r) : value(v), row_ids(r) {}
    T value;

    // In many cases, we only have a single entry per value. For TPC-H, only 5% would call
    // `PosList::push_back`. By having this variant, we can save us the cost of allocating
    // heap storage for a single value.
    boost::variant<RowID, PosList> row_ids;
  };

  /*
  function to place a key in one of its possible positions
  tableID: table in which key has to be placed, also equal
  to function according to which key must be hashed
  cnt: number of times function has already been called
  in order to place the first input key
  n: maximum number of times function can be recursively
  called before stopping and declaring presence of cycle
  */
  void place(std::optional<HashElement> element, int hash_function, size_t iterations) {
    /*
    We were not able to reproduce this case with the current setting (3 hash functions). With 3 hash functions the
    hash table will have a maximum load of 33%, which should be less enough to avoid cycles at all. In theory there
    shouldn't be any cycles up to a load of 91%, comp. http://www.ru.is/faculty/ulfar/CuckooHash.pdf
    */
    Assert((iterations != _input_table_size),
           "There is a cycle in Cuckoo. Need to rehash with different hash functions");

    /*
    Check if another element is already present at the position.
    If yes, we need to insert the older one in another hash table.
    If no, we can simply add the new element.
    */
    auto position = hash(hash_function, element->value);
    auto& hashtable = _hashtables[hash_function];

    auto old_element = std::move(hashtable[position]);
    hashtable[position] = std::move(element);
    if (old_element) {
      place(std::move(old_element), (hash_function + 1) % NUMBER_OF_HASH_FUNCTIONS, iterations + 1);
    }
  }

  /*
  return hashed value for a value
  */
  template <typename R>
  int hash(int seed, R value) const {
    // avoid a seed of 0 and increase it by some factor, 10 seems to be working fine
    return murmur2<R>(value, (seed + 1) * 10) % _input_table_size;
  }

  size_t _input_table_size;
  std::vector<std::vector<std::optional<HashElement>>> _hashtables;
};
}  // namespace opossum
