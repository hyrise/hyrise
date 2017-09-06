#include "composite_group_key_index.hpp"

#include <climits>
#include <cstdint>

#include <algorithm>
#include <iterator>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "variable_length_key_proxy.hpp"

#include "storage/base_attribute_vector.hpp"
#include "storage/base_dictionary_column.hpp"

#include "utils/assert.hpp"

namespace opossum {

CompositeGroupKeyIndex::CompositeGroupKeyIndex(const std::vector<std::shared_ptr<BaseColumn>> &indexed_columns) {
  DebugAssert(!indexed_columns.empty(), "CompositeGroupKeyIndex requires at least one column to be indexed.");

  if (IS_DEBUG) {
    auto firstSize = indexed_columns.front()->size();
    [[gnu::unused]] auto haveAllColumnsSameSize =
        std::all_of(indexed_columns.cbegin(), indexed_columns.cend(),
                    [firstSize](const auto &column) { return column->size() == firstSize; });

    DebugAssert(haveAllColumnsSameSize,
                "CompositeGroupKey requires same length of all columns that should be indexed.");
  }

  // cast and check columns
  _indexed_columns.reserve(indexed_columns.size());
  for (const auto &column : indexed_columns) {
    auto dict_column = std::dynamic_pointer_cast<BaseDictionaryColumn>(column);
    DebugAssert(static_cast<bool>(dict_column), "CompositeGroupKeyIndex only works with DictionaryColumns");
    _indexed_columns.emplace_back(dict_column);
  }

  // retrive amount of memory consumed by each concatenated key
  CompositeKeyLength bytes_per_key = std::accumulate(
      _indexed_columns.begin(), _indexed_columns.end(), 0u,
      [](auto key_length, const auto &column) { return key_length + column->attribute_vector()->width(); });

  // create concatenated keys and save their positions
  // at this point duplicated keys may be created, they will be handled later
  auto column_size = _indexed_columns.front()->size();
  auto keys = std::vector<VariableLengthKey>(column_size);
  _position_list.resize(column_size);

  for (ChunkOffset chunkOffset = 0; chunkOffset < column_size; ++chunkOffset) {
    auto concatenated_key = VariableLengthKey(bytes_per_key);
    for (const auto &column : _indexed_columns) {
      const auto &attribute_vector = column->attribute_vector();
      concatenated_key.shift_and_set(attribute_vector->get(chunkOffset), attribute_vector->width() * CHAR_BIT);
    }
    keys[chunkOffset] = std::move(concatenated_key);
    _position_list[chunkOffset] = chunkOffset;
  }

  // sort keys and their positions
  std::sort(_position_list.begin(), _position_list.end(),
            [&keys](auto lhs, auto rhs) { return keys[lhs] < keys[rhs]; });

  _keys = VariableLengthKeyStore(column_size, bytes_per_key);
  for (ChunkOffset chunkOffset = 0; chunkOffset < column_size; ++chunkOffset) {
    _keys[chunkOffset] = keys[_position_list[chunkOffset]];
  }

  // create offsets to unique keys
  _key_offsets.reserve(column_size);
  _key_offsets.emplace_back(0);
  for (ChunkOffset chunkOffset = 1; chunkOffset < column_size; ++chunkOffset) {
    if (_keys[chunkOffset] != _keys[chunkOffset - 1]) _key_offsets.emplace_back(chunkOffset);
  }
  _key_offsets.shrink_to_fit();

  // remove duplicated keys
  auto unique_keys_end = std::unique(_keys.begin(), _keys.end());
  _keys.erase(unique_keys_end, _keys.end());
  _keys.shrink_to_fit();
}

BaseIndex::Iterator CompositeGroupKeyIndex::_cbegin() const { return _position_list.cbegin(); }

BaseIndex::Iterator CompositeGroupKeyIndex::_cend() const { return _position_list.cend(); }

BaseIndex::Iterator CompositeGroupKeyIndex::_lower_bound(const std::vector<AllTypeVariant> &values) const {
  auto composite_key = _create_composite_key(values, false);
  return _get_position_iterator_for_key(composite_key);
}

BaseIndex::Iterator CompositeGroupKeyIndex::_upper_bound(const std::vector<AllTypeVariant> &values) const {
  auto composite_key = _create_composite_key(values, true);
  return _get_position_iterator_for_key(composite_key);
}

VariableLengthKey CompositeGroupKeyIndex::_create_composite_key(const std::vector<AllTypeVariant> &values,
                                                                bool is_upper_bound) const {
  auto result = VariableLengthKey(_keys.key_size());

  // retrieve the partial keys for every value except for the last one and append them into one partial-key
  for (size_t column = 0; column < values.size() - 1; ++column) {
    auto partial_key = _indexed_columns[column]->lower_bound(values[column]);
    auto bits_of_partial_key = _indexed_columns[column]->attribute_vector()->width() * CHAR_BIT;
    result.shift_and_set(partial_key, bits_of_partial_key);
  }

  // retrieve the partial key for the last value (depending on whether we have a lower- or upper-bound-query)
  // and append it to the previously created partial key to obtain the key containing all provided values
  const auto &column_for_last_value = _indexed_columns[values.size() - 1];
  auto &&partial_key = is_upper_bound ? column_for_last_value->upper_bound(values.back())
                                      : column_for_last_value->lower_bound(values.back());
  auto bits_of_partial_key = column_for_last_value->attribute_vector()->width() * CHAR_BIT;
  result.shift_and_set(partial_key, bits_of_partial_key);

  // fill empty space of key with zeros if less values than columns were provided
  auto empty_bits =
      std::accumulate(_indexed_columns.cbegin() + values.size(), _indexed_columns.cend(), static_cast<uint8_t>(0u),
                      [](auto value, auto column) { return value + column->attribute_vector()->width() * CHAR_BIT; });
  result <<= empty_bits;

  return result;
}

BaseIndex::Iterator CompositeGroupKeyIndex::_get_position_iterator_for_key(const VariableLengthKey &key) const {
  // get an iterator pointing to the search-key in the keystore
  // (use always lower_bound() since the search method is already handled within creation of composite key)
  auto key_iter = std::lower_bound(_keys.cbegin(), _keys.cend(), key);
  if (key_iter == _keys.cend()) return _position_list.cend();

  // get the start position in the position-vector, ie the offset, by getting the offset_iterator for the key
  // (which is at the same position as the iterator for the key in the keystore)
  auto offset_iter = _key_offsets.cbegin();
  std::advance(offset_iter, std::distance(_keys.cbegin(), key_iter));

  // get an iterator pointing to that start position
  auto position_iter = _position_list.cbegin();
  std::advance(position_iter, *offset_iter);

  return position_iter;
}

std::vector<std::shared_ptr<BaseColumn>> CompositeGroupKeyIndex::_get_index_columns() const {
  auto result = std::vector<std::shared_ptr<BaseColumn>>();
  result.reserve(_indexed_columns.size());
  for (auto &&indexed_column : _indexed_columns) {
    result.emplace_back(indexed_column);
  }
  return result;
}

}  // namespace opossum
