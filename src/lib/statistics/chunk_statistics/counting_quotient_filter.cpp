#include "counting_quotient_filter.hpp"

#include <cmath>
#include <iostream>
#include <string>

#include "utils/murmur_hash.hpp"
#include "resolve_type.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "types.hpp"

using namespace gqf2; // NOLINT
using namespace gqf4; // NOLINT
using namespace gqf8; // NOLINT
using namespace gqf16; // NOLINT
using namespace gqf32; // NOLINT

namespace opossum {

template <typename ElementType>
CountingQuotientFilter<ElementType>::CountingQuotientFilter(uint8_t quotient_size, uint8_t remainder_size) {
  Assert(quotient_size > 0, "quotient size can not be zero.");
  Assert(quotient_size + static_cast<uint8_t>(remainder_size) <= 64, "The hash length can not exceed 64 bits.");
  Assert(remainder_size == 2 || remainder_size == 4 || remainder_size == 8 || remainder_size == 16
    || remainder_size == 32, "remainder size must be 2, 4, 8, 16, or 32");

  _remainder_size = remainder_size;
  _number_of_slots = std::pow(2, quotient_size);
  _hash_bits = quotient_size + remainder_size;

if (remainder_size == 2) {
    _quotient_filter = gqf2::quotient_filter();
  } else if (remainder_size == 4) {
    _quotient_filter = gqf4::quotient_filter();
  } else if (remainder_size == 8) {
    _quotient_filter = gqf8::quotient_filter();
  } else if (remainder_size == 16) {
    _quotient_filter = gqf16::quotient_filter();
  } else if (remainder_size == 32) {
    _quotient_filter = gqf32::quotient_filter();
  }
  boost::apply_visitor([&](auto& filter) {qf_init(&filter, _number_of_slots, _hash_bits, 0);}, _quotient_filter);
}

template <typename ElementType>
CountingQuotientFilter<ElementType>::~CountingQuotientFilter() {
  boost::apply_visitor([&](auto& filter) {qf_destroy(&filter);}, _quotient_filter);
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::insert(ElementType element, uint64_t count) {
  uint64_t bitmask = static_cast<uint64_t>(std::pow(2, _hash_bits)) - 1;
  uint64_t hash = bitmask & _hash(element);
  for (size_t i = 0; i < count; i++) {
    boost::apply_visitor([&](auto& filter) {qf_insert(&filter, hash, 0, 1);}, _quotient_filter);
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
bool CountingQuotientFilter<ElementType>::can_prune(const AllTypeVariant& value,
    const PredicateCondition predicate_type) const {
  DebugAssert(predicate_type == PredicateCondition::Equals, "CQF only supports equality predicates");
  return count_all_type(value) == 0;
}

template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::count(ElementType element) const {
  uint64_t bitmask = static_cast<uint64_t>(std::pow(2, _hash_bits)) - 1;
  uint64_t hash = bitmask & _hash(element);
  uint64_t count;
  boost::apply_visitor([&](auto& filter) {count = qf_count_key_value(&filter, hash, 0);}, _quotient_filter);
  return count;
}

/**
* Computes the hash for a value.
**/
template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::_hash(ElementType value) const {
  auto hash = murmur2<ElementType>(value, _seed);
  return static_cast<uint64_t>(hash);
}

/**
* Computes the hash for a string.
**/
template <>
uint64_t CountingQuotientFilter<std::string>::_hash(std::string value) const {
  auto hash = murmur_hash2(value.data(), value.length(), _seed);
  return static_cast<uint64_t>(hash);
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::populate(std::shared_ptr<const BaseSegment> segment) {
  resolve_segment_type<ElementType>(*segment, [&](const auto& typed_segment) {
    auto iterable_left = create_iterable_from_segment<ElementType>(typed_segment);
    iterable_left.for_each([&](const auto& value) {
      if (value.is_null()) return;
      insert(value.value());
    });
  });
}

template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::memory_consumption() const {
  uint64_t consumption = 0;
  boost::apply_visitor([&](auto& filter) {consumption += qf_memory_consumption(filter);}, _quotient_filter);
  return consumption;
}

template <typename ElementType>
double CountingQuotientFilter<ElementType>::load_factor() const {
  double load_factor = 0;
  boost::apply_visitor([&](auto& filter) {load_factor = filter.noccupied_slots / static_cast<double>(filter.nslots);},
                        _quotient_filter);
  return load_factor;
}

template <typename ElementType>
bool CountingQuotientFilter<ElementType>::is_full() const {
  return load_factor() > 0.99;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(CountingQuotientFilter);

}  // namespace opossum
