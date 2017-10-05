#pragma once

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
    _hashtables.resize(NUMBER_OF_HASH_FUNCTIONS, std::vector<std::shared_ptr<HashElement>>(input_table_size));
  }

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  HashTable(HashTable &&) = default;
  HashTable &operator=(HashTable &&) = default;

  /*
  Insert a new element into hashtable
  */
  void put(const T value, const RowID row_id) {
    // Check whether value is already in hashtable, then just add row id
    for (size_t i = 0; i < NUMBER_OF_HASH_FUNCTIONS; i++) {
      auto position = hash<T>(i, value);
      auto elem = _hashtables[i][position];
      if (elem != nullptr && value_equal(elem->value, value)) {
        elem->row_ids->push_back(row_id);
        return;
      }
    }
    auto element =
        std::make_shared<HashElement>(HashElement{value, std::make_shared<PosList>(pmr_vector<RowID>{row_id})});
    place(element, 0, 0);
  }

  /*
  Retrieve an element from hashtable returning true or false indicating the success.
  All the matching RowIDs are returned in row_ids.
  */
  template <typename S>
  std::shared_ptr<PosList> get(S value) {
    for (size_t i = 0; i < NUMBER_OF_HASH_FUNCTIONS; i++) {
      auto position = hash<S>(i, value);
      auto elem = _hashtables[i][position];
      if (elem != nullptr && value_equal(elem->value, value)) {
        return elem->row_ids;
      }
    }
    return nullptr;
  }

 protected:
  /*
  We use this struct internally for storing data. It should not be exposed to other classes.
  */
  struct HashElement {
    T value;
    std::shared_ptr<PosList> row_ids;
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
  void place(std::shared_ptr<HashElement> element, int hash_function, size_t iterations) {
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
    If no, we can simply add the ned eleemnt.
    */
    auto position = hash(hash_function, element->value);
    auto &hashtable = _hashtables[hash_function];

    auto old_element = hashtable[position];
    if (old_element != nullptr) {
      hashtable[position] = element;
      place(old_element, (hash_function + 1) % NUMBER_OF_HASH_FUNCTIONS, iterations + 1);
    } else {
      hashtable[position] = element;
    }
  }

  /*
  return hashed value for a value
  */
  template <typename R>
  int hash(int seed, R value) {
    // avoid a seed of 0 and increase it by some factor, 10 seems to be working fine
    return murmur2<R>(value, (seed + 1) * 10) % _input_table_size;
  }

  size_t _input_table_size;
  std::vector<std::vector<std::shared_ptr<HashElement>>> _hashtables;
};
}  // namespace opossum
