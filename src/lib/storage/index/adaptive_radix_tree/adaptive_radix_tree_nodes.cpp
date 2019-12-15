#include "adaptive_radix_tree_nodes.hpp"

#include <algorithm>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "adaptive_radix_tree_index.hpp"
#include "storage/index/abstract_index.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

constexpr uint8_t INVALID_INDEX = 255u;

/**
 *
 * ARTNode4 has two arrays of length 4:
 *  - _partial_keys stores the contained partial_keys of its children
 *  - _children stores pointers to the children
 * _partial_key[i] is the partial_key for child _children[i]
 * default value of the _partial_keys array is 255u
 */

ARTNode4::ARTNode4(std::vector<std::pair<uint8_t, std::shared_ptr<ARTNode>>>& children) {
  std::sort(children.begin(), children.end(),
            [](const std::pair<uint8_t, std::shared_ptr<ARTNode>>& left,
               const std::pair<uint8_t, std::shared_ptr<ARTNode>>& right) { return left.first < right.first; });
  _partial_keys.fill(INVALID_INDEX);
  for (auto i = size_t{0}; i < children.size(); ++i) {
    _partial_keys[i] = children[i].first;
    _children[i] = children[i].second;
  }
}

/**
 *
 * searches the child that satisfies the query (lower_bound/ upper_bound + partial_key)
 * calls the appropriate function on the child
 * in case the partial_key is not contained in this node, the query has to be adapted
 *
 *                          04 | 06 | 07 | 08
 *                           |    |    |    |
 *                   |-------|    |    |    |---------|
 *                   |            |    |              |
 *        01| 02 |ff|ff  01|02|03|04  06|07|bb|ff    00|a2|b7|fe
 *         |  |    |      |  |  |  |   |  |  |        |  |  |  |
 *
 * case0:  partial_key (e.g. 06) matches a value in the node
 *           call the query-function on the child at the matching position
 * case1a: partial_key (e.g. 09) is larger than any value in the node which is full
 *           call this->end() which calls end() on the last child
 * case1b: partial_key (e.g. e0 in child at 07) is larger than any value in the node which is not full (last ff does
 *              not have a matching child, it simply is the default value)
 *          call this->end() which calls end() on the last child
 * case2:  partial_key (e.g. 05) is not contained, but smaller than a value in the node
 *           call begin() on the next larger child (e.g. 06)
 **/

AbstractIndex::Iterator ARTNode4::_delegate_to_child(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth,
                                                     const std::function<Iterator(size_t, size_t)>& function) const {
  auto partial_key = key[depth];
  for (uint8_t partial_key_id = 0; partial_key_id < 4; ++partial_key_id) {
    if (_partial_keys[partial_key_id] < partial_key) continue;                                   // key not found yet
    if (_partial_keys[partial_key_id] == partial_key) return function(partial_key_id, ++depth);  // case0
    if (!_children[partial_key_id]) return end();  // no more keys available, case1b
    return _children[partial_key_id]->begin();     // case2
  }
  return end();  // case1a
}

AbstractIndex::Iterator ARTNode4::lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const {
  return _delegate_to_child(
      key, depth, [&key, this](size_t i, size_t new_depth) { return _children[i]->lower_bound(key, new_depth); });
}

AbstractIndex::Iterator ARTNode4::upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth) const {
  return _delegate_to_child(
      key, depth, [&key, this](size_t i, size_t new_depth) { return _children[i]->upper_bound(key, new_depth); });
}

AbstractIndex::Iterator ARTNode4::begin() const { return _children[0]->begin(); }

AbstractIndex::Iterator ARTNode4::end() const {
  for (uint8_t i = 4; i > 0; --i) {
    if (_children[i - 1]) {
      return _children[i - 1]->end();
    }
  }
  Fail("Empty _children array in ARTNode4 should never happen");
}

/**
 *
 * ARTNode16 has two arrays of length 16, very similar to ARTNode4:
 *  - _partial_keys stores the contained partial_keys of its children
 *  - _children stores pointers to the children
 * _partial_key[i] is the partial_key for child _children[i]
 * default value of the _partial_keys array is 255u
 *
 */

ARTNode16::ARTNode16(std::vector<std::pair<uint8_t, std::shared_ptr<ARTNode>>>& children) {
  std::sort(children.begin(), children.end(),
            [](const std::pair<uint8_t, std::shared_ptr<ARTNode>>& left,
               const std::pair<uint8_t, std::shared_ptr<ARTNode>>& right) { return left.first < right.first; });
  _partial_keys.fill(INVALID_INDEX);
  for (auto i = size_t{0}; i < children.size(); ++i) {
    _partial_keys[i] = children[i].first;
    _children[i] = children[i].second;
  }
}

/**
 * searches the child that satisfies the query (lower_bound/ upper_bound + partial_key)
 * calls the appropriate function on the child
 * in case the partial_key is not contained in this node, the query has to be adapted
 *
 *                          04|..|06 |07|..|e2
 *                           |    |    |    |
 *                   |-------|    |    |    |---------|
 *                   |            |    |              |
 *        01| 02 |ff|ff  01|02|03|04  06|07|bb|ff    00|a2|b7|..|fa|ff|ff
 *                                                    |  |  | ||  |
 *
 * case0:  partial_key (e.g. 06) matches a value in the node
 *           call the query-function on the child at the matching position
 * case1a: partial_key (e.g. fa) is larger than any value in the node which is full
 *           call this->end() which calls end() on the last child
 * case1b: partial_key (e.g. fb in child at e2) is larger than any value in the node which is not full (ffs do
 *              not have matching children in this example, it simply is the default value)
 *          call this->end() which calls end() on the last child
 * case2:  partial_key (e.g. 05) is not contained, but smaller than a value in the node
 *           call begin() on the next larger child (e.g. 06)
 **/

AbstractIndex::Iterator ARTNode16::_delegate_to_child(
    const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth,
    const std::function<Iterator(std::iterator_traits<std::array<uint8_t, 16>::iterator>::difference_type, size_t)>&
        function) const {
  auto partial_key = key[depth];
  auto partial_key_iterator = std::lower_bound(_partial_keys.begin(), _partial_keys.end(), partial_key);
  auto partial_key_pos = std::distance(_partial_keys.begin(), partial_key_iterator);

  if (*partial_key_iterator == partial_key) {
    return function(partial_key_pos, ++depth);  // case0
  }
  if (partial_key_pos >= 16) {
    return end();  // case 1a
  }
  if (_children[partial_key_pos]) {
    return _children[partial_key_pos]->begin();  // case2
  }
  return end();  // case1b
}

AbstractIndex::Iterator ARTNode16::lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key,
                                               size_t depth) const {
  return _delegate_to_child(
      key, depth,
      [&key, this](std::iterator_traits<std::array<uint8_t, 16>::iterator>::difference_type partial_key_pos,
                   size_t new_depth) { return _children[partial_key_pos]->lower_bound(key, new_depth); });
}

AbstractIndex::Iterator ARTNode16::upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key,
                                               size_t depth) const {
  return _delegate_to_child(
      key, depth,
      [&key, this](std::iterator_traits<std::array<uint8_t, 16>::iterator>::difference_type partial_key_pos,
                   size_t new_depth) { return _children[partial_key_pos]->upper_bound(key, new_depth); });
}

AbstractIndex::Iterator ARTNode16::begin() const { return _children[0]->begin(); }

/**
 * _end searches the child with the largest partial key == the last child in the _children array.
 * As the _partial_keys array is filled with 255u per default, we expect the largest child at the index right in front
 * of the first entry with 255u. But 255u can also be a valid partial_key: In this case the _children array contains a
 * pointer at this index as a means to differentiate the two cases.
 */

AbstractIndex::Iterator ARTNode16::end() const {
  auto partial_key_iterator = std::lower_bound(_partial_keys.begin(), _partial_keys.end(), INVALID_INDEX);
  auto partial_key_pos = std::distance(_partial_keys.begin(), partial_key_iterator);
  if (!_children[partial_key_pos]) {
    // there does not exist a child with partial_key 255u, we take the partial_key in front of it
    return _children[partial_key_pos - 1]->end();
  } else {
    // there exists a child with partial_key 255u
    return _children[partial_key_pos]->end();
  }
}

/**
 *
 * ARTNode48 has two arrays:
 *  - _index_to_child of length 256 that can be directly addressed
 *  - _children of length 48 stores pointers to the children
 * _index_to_child[partial_key] stores the index for the child in _children
 * default value of the _index_to_child array is 255u. This is safe as the maximum value set in _index_to_child will be
 * 47 as this is the maximum index for _children.
 */

ARTNode48::ARTNode48(const std::vector<std::pair<uint8_t, std::shared_ptr<ARTNode>>>& children) {
  _index_to_child.fill(INVALID_INDEX);
  for (auto i = size_t{0}; i < children.size(); ++i) {
    _index_to_child[children[i].first] = static_cast<uint8_t>(i);
    _children[i] = children[i].second;
  }
}

/**
 * searches the child that satisfies the query (lower_bound/ upper_bound + partial_key)
 * calls the appropriate function on the child
 * in case the partial_key is not contained in this node, the query has to be adapted
 *
 * _index_to_child:
 *      00|01|02|03|04|05|06|07|08|09|0a|...| fd |fe|ff|  index
 *      ff|ff|00|ff|ff|01|02|03|ff|04|ff|...|0x30|ff|ff|  value
 *
 * _children
 *      00|01|02|03|04|05|06|07|08|09|0a|...|0x30|
 *       |  |  |  |  |  |  |  |  |  |  | |||  |
 *
 *
 * case0:  partial_key (e.g. 05) matches a value in the node
 *           call the query-function on _children[_index_to_child[partial_key]
 * case1: partial_key (e.g. fe) is larger than any value in the node
 *           call this->end() which calls end() on the last child
 * case2:  partial_key (e.g. 04) is not contained, but smaller than a value in the node
 *           call begin() on the next larger child (e.g. 05)
 *
 * In order to find the next larger/ last child, we have to iterate through the _index_to_child array
 * This is expensive as the array is sparsely populated (at max 48 entries).
 * For the moment, all entries in _children are sorted, as we only bulk_insert records, so we could just iterate through
 * _children instead.
 * But this sorting is not necessarily the case when inserting is allowed (_index_to_child[new_partial_key] would get
 * the largest free index in _children).
 * For future safety, we decided against this more efficient implementation.
 *
 **/

AbstractIndex::Iterator ARTNode48::_delegate_to_child(const AdaptiveRadixTreeIndex::BinaryComparable& key, size_t depth,
                                                      const std::function<Iterator(uint8_t, size_t)>& function) const {
  auto partial_key = key[depth];
  if (_index_to_child[partial_key] != INVALID_INDEX) {
    // case0
    return function(partial_key, ++depth);
  }
  for (uint16_t i = partial_key + 1; i < 256u; ++i) {
    if (_index_to_child[i] != INVALID_INDEX) {
      // case2
      return _children[_index_to_child[i]]->begin();
    }
  }
  // case1
  return end();
}

AbstractIndex::Iterator ARTNode48::lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key,
                                               size_t depth) const {
  return _delegate_to_child(key, depth, [&key, this](uint8_t partial_key, size_t new_depth) {
    return _children[_index_to_child[partial_key]]->lower_bound(key, new_depth);
  });
}

AbstractIndex::Iterator ARTNode48::upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key,
                                               size_t depth) const {
  return _delegate_to_child(key, depth, [&key, this](uint8_t partial_key, size_t new_depth) {
    return _children[_index_to_child[partial_key]]->upper_bound(key, new_depth);
  });
}

AbstractIndex::Iterator ARTNode48::begin() const {
  for (auto index : _index_to_child) {
    if (index != INVALID_INDEX) {
      return _children[index]->begin();
    }
  }
  Fail("Empty _index_to_child array in ARTNode48 should never happen");
}

AbstractIndex::Iterator ARTNode48::end() const {
  for (uint8_t i = static_cast<uint8_t>(_index_to_child.size()) - 1; i > 0; --i) {
    if (_index_to_child[i] != INVALID_INDEX) {
      return _children[_index_to_child[i]]->begin();
    }
  }
  Fail("Empty _index_to_child array in ARTNode48 should never happen");
}

/**
 *
 * ARTNode256 has only one array: _children; which stores pointers to the children and can be directly addressed.
 *
 */

ARTNode256::ARTNode256(const std::vector<std::pair<uint8_t, std::shared_ptr<ARTNode>>>& children) {
  for (const auto& child : children) {
    _children[child.first] = child.second;
  }
}

/**
 * searches the child that satisfies the query (lower_bound/ upper_bound + partial_key)
 * calls the appropriate function on the child
 * in case the partial_key is not contained in this node, the query has to be adapted
 *
 *
 * _children
 *      00|01|02|03|04|05|06|07|08|09|0a|...|fd|fe|ff|
 *       |  |  |  |        |     |     | |||  |
 *
 *
 * case0:  _children[partial_key] (e.g. 03) contains a pointer to a child
 *           call the query-function on _children[partial_key]
 * case1: _children[partial_key] (e.g. fe) does contain a nullptr and so does every position afterwards
 *           call this->end() which calls end() on the last child (fd)
 * case2:  _children[partial_key] (e.g. 04)  does contain a nullptr, but there are valid pointers to children afterwards
 *           call begin() on the next larger child (e.g. 06)
 *
 * In order to find the next larger/ last child, we have to iterate through the _children array
 * This is not as expensive as for ARTNode48 as the array has > 48 entries
 *
 **/

AbstractIndex::Iterator ARTNode256::_delegate_to_child(const AdaptiveRadixTreeIndex::BinaryComparable& key,
                                                       size_t depth,
                                                       const std::function<Iterator(uint8_t, size_t)>& function) const {
  auto partial_key = key[depth];
  if (_children[partial_key]) {
    // case0
    return function(partial_key, ++depth);
  }
  for (uint16_t i = partial_key + 1; i < 256u; ++i) {
    if (_children[i]) {
      // case2
      return _children[i]->begin();
    }
  }
  // case1
  return end();
}

AbstractIndex::Iterator ARTNode256::upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key,
                                                size_t depth) const {
  return _delegate_to_child(key, depth, [&key, this](uint8_t partial_key, size_t new_depth) {
    return _children[partial_key]->upper_bound(key, new_depth);
  });
}

AbstractIndex::Iterator ARTNode256::lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable& key,
                                                size_t depth) const {
  return _delegate_to_child(key, depth, [&key, this](uint8_t partial_key, size_t new_depth) {
    return _children[partial_key]->lower_bound(key, new_depth);
  });
}

AbstractIndex::Iterator ARTNode256::begin() const {
  for (const auto& child : _children) {
    if (child) {
      return child->begin();
    }
  }
  Fail("Empty _children array in ARTNode256 should never happen");
}

AbstractIndex::Iterator ARTNode256::end() const {
  for (int16_t i = static_cast<int16_t>(_children.size()) - 1; i >= 0; --i) {
    if (_children[i]) {
      return _children[i]->begin();
    }
  }
  Fail("Empty _children array in ARTNode256 should never happen");
}

Leaf::Leaf(AbstractIndex::Iterator& lower, AbstractIndex::Iterator& upper) : _begin(lower), _end(upper) {}

AbstractIndex::Iterator Leaf::lower_bound(const AdaptiveRadixTreeIndex::BinaryComparable&, size_t) const {
  return _begin;
}

AbstractIndex::Iterator Leaf::upper_bound(const AdaptiveRadixTreeIndex::BinaryComparable&, size_t) const {
  return _end;
}

AbstractIndex::Iterator Leaf::begin() const { return _begin; }

AbstractIndex::Iterator Leaf::end() const { return _end; }

}  // namespace opossum
