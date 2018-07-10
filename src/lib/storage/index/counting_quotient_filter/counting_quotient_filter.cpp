#include "counting_quotient_filter.hpp"
//#include "utils/xxhash.hpp"
#include "utils/murmur_hash.hpp"
#include "resolve_type.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/create_iterable_from_column.hpp"

#include <cmath>
#include <iostream>
#include <string>

namespace opossum {

template <typename ElementType>
CountingQuotientFilter<ElementType>::CountingQuotientFilter(uint8_t quotient_bits, uint8_t remainder_bits) {
  Assert(quotient_bits > 0, "quotient size can not be zero.");
  Assert(quotient_bits + remainder_bits <= 64, "The hash length can not exceed 64 bits.");

  _quotient_bits = quotient_bits;
  _remainder_bits = remainder_bits;
  _number_of_slots = std::pow(2, _quotient_bits);
  _hash_bits = _quotient_bits + _remainder_bits;
  _quotient_filter = quotient_filter();
  // (QF*, nslots, key_bits, value_bits, lockingmode, hashmode , seed)
  qf_malloc(&_quotient_filter.value(), _number_of_slots, _hash_bits, 0, LOCKS_FORBIDDEN, DEFAULT, 0);
}

template <typename ElementType>
CountingQuotientFilter<ElementType>::~CountingQuotientFilter() {
  if (_quotient_filter.has_value()) {
    qf_destroy(&_quotient_filter.value());
  }
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::insert(ElementType element, uint64_t count) {
  //std::cout << "load factor: " << load_factor() << std::endl;
  uint64_t bitmask = static_cast<uint64_t>(std::pow(2, _hash_bits)) - 1;
  uint64_t hash = bitmask & _hash(element);
  for (uint64_t i = 0; i < count; i++) {
    qf_insert(&_quotient_filter.value(), hash, 0, 1);
  }
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::insert(ElementType element) {
  insert(element, 1);
}

template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::count_all_type(AllTypeVariant value) const {
  DebugAssert(value.type() == typeid(ElementType), "Value does not have the same type as the filter elements");
  return count(type_cast<ElementType>(value));
}

template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::count(ElementType element) const {
  uint64_t bitmask = static_cast<uint64_t>(std::pow(2, _hash_bits)) - 1;
  uint64_t hash = bitmask & _hash(element);
  return qf_count_key_value(&_quotient_filter.value(), hash, 0);
}

/**
* Computes the hash for a value.
**/
template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::_hash(ElementType value) const {
  //auto hash = xxh::xxhash<64, ElementType>(&value, 1, _seed);
  auto hash = murmur2<ElementType>(value, _seed);
  return static_cast<uint64_t>(hash);
}

/**
* Computes the hash for a string.
**/
template <>
uint64_t CountingQuotientFilter<std::string>::_hash(std::string value) const {
  //auto hash = xxh::xxhash<64, char>(value.data(), value.length(), _seed);
  auto hash = murmur_hash2(value.data(), value.length(), _seed);
  return static_cast<uint64_t>(hash);
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::populate(std::shared_ptr<const BaseColumn> column) {
  resolve_column_type<ElementType>(*column, [&](const auto& typed_column) {
    auto iterable_left = create_iterable_from_column<ElementType>(typed_column);
    iterable_left.for_each([&](const auto& value) {
      if (value.is_null()) return;
      insert(value.value());
    });
  });
}

/*
template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::memory_consumption() const {
  return memory_consumption(_quotient_filter.value());
}
*/

template <typename ElementType>
double CountingQuotientFilter<ElementType>::load_factor() const {
  return _quotient_filter.value().metadata->noccupied_slots /
            static_cast<double>(_quotient_filter.value().metadata->nslots);
}

template <typename ElementType>
bool CountingQuotientFilter<ElementType>::is_full() const {
  return load_factor() > 0.99;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(CountingQuotientFilter);

} // namespace opossum
