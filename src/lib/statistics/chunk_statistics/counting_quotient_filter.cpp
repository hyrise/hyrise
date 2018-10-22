#include "counting_quotient_filter.hpp"

#include <cmath>
#include <iostream>
#include <string>

#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace gqf2;   // NOLINT
using namespace gqf4;   // NOLINT
using namespace gqf8;   // NOLINT
using namespace gqf16;  // NOLINT
using namespace gqf32;  // NOLINT

namespace opossum {

template <typename ElementType>
CountingQuotientFilter<ElementType>::CountingQuotientFilter(const size_t quotient_size, const size_t remainder_size)
    : _hash_bits(quotient_size + remainder_size) {
  Assert(quotient_size > 0, "Quotient size can not be zero.");
  Assert(_hash_bits <= 64u, "Hash length can not exceed 64 bits.");

  if (remainder_size == 2) {
    _quotient_filter = gqf2::quotient_filter{};
  } else if (remainder_size == 4) {
    _quotient_filter = gqf4::quotient_filter{};
  } else if (remainder_size == 8) {
    _quotient_filter = gqf8::quotient_filter{};
  } else if (remainder_size == 16) {
    _quotient_filter = gqf16::quotient_filter{};
  } else if (remainder_size == 32) {
    _quotient_filter = gqf32::quotient_filter{};
  } else {
    Fail("Invalid remainder remainder_size");
  }

  const auto number_of_slots = std::pow(2, quotient_size);
  boost::apply_visitor([&](auto& filter) { qf_init(&filter, number_of_slots, _hash_bits, 0); }, _quotient_filter);
}

template <typename ElementType>
CountingQuotientFilter<ElementType>::~CountingQuotientFilter() {
  boost::apply_visitor([&](auto& filter) { qf_destroy(&filter); }, _quotient_filter);
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::insert(ElementType value, size_t count) {
  const auto bitmask = static_cast<size_t>(std::pow(2, _hash_bits)) - 1;
  const auto hash = bitmask & _hash(value);
  for (size_t idx = 0; idx < count; ++idx) {
    boost::apply_visitor([&](auto& filter) { qf_insert(&filter, hash, 0, 1); }, _quotient_filter);
  }
}

template <typename ElementType>
size_t CountingQuotientFilter<ElementType>::count(const AllTypeVariant& value) const {
  DebugAssert(value.type() == typeid(ElementType), "Value does not have the same type as the filter elements");
  return count(type_cast<ElementType>(value));
}

template <typename ElementType>
bool CountingQuotientFilter<ElementType>::can_prune(const PredicateCondition predicate_type,
                                                    const AllTypeVariant& value,
                                                    const std::optional<AllTypeVariant>& variant_value2) const {
  DebugAssert(predicate_type == PredicateCondition::Equals && !variant_value2, "CQF only supports equality predicates");
  return count(value) == 0;
}

template <typename ElementType>
size_t CountingQuotientFilter<ElementType>::count(const ElementType& value) const {
  const auto bitmask = static_cast<uint64_t>(std::pow(2, _hash_bits)) - 1;
  const auto hash = bitmask & _hash(value);

  auto count = size_t{0};
  boost::apply_visitor([&](auto& filter) { count = qf_count_key_value(&filter, hash, 0); }, _quotient_filter);
  return count;
}

template <typename ElementType>
size_t CountingQuotientFilter<ElementType>::_hash(const ElementType& value) const {
  return std::hash<ElementType>{}(value);
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::populate(const std::shared_ptr<const BaseSegment>& segment) {
  resolve_segment_type<ElementType>(*segment, [&](const auto& typed_segment) {
    auto segment_iterable = create_iterable_from_segment<ElementType>(typed_segment);
    segment_iterable.for_each([&](const auto& value) {
      if (value.is_null()) return;
      insert(value.value());
    });
  });
}

template <typename ElementType>
size_t CountingQuotientFilter<ElementType>::memory_consumption() const {
  size_t consumption = 0;
  boost::apply_visitor([&](auto& filter) { consumption = qf_memory_consumption(filter); }, _quotient_filter);
  return consumption;
}

template <typename ElementType>
float CountingQuotientFilter<ElementType>::load_factor() const {
  auto load_factor = 0.f;
  boost::apply_visitor([&](auto& filter) { load_factor = filter.noccupied_slots / static_cast<float>(filter.nslots); },
                       _quotient_filter);
  return load_factor;
}

template <typename ElementType>
bool CountingQuotientFilter<ElementType>::is_full() const {
  return load_factor() > 0.99f;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(CountingQuotientFilter);

}  // namespace opossum
