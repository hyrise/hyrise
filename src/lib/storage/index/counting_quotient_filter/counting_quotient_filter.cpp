#include "counting_quotient_filter.hpp"
#include "utils/xxhash.hpp"
#include "utils/murmur_hash.hpp"
#include "resolve_type.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"

#include <cmath>
#include <iostream>
#include <string>

namespace opossum {

template <typename ElementType>
CountingQuotientFilter<ElementType>::CountingQuotientFilter(uint8_t quotient_bits, uint8_t remainder_bits) {
  DebugAssert(remainder_bits == 2 || remainder_bits == 4 || remainder_bits == 8 || remainder_bits == 16
              || remainder_bits == 32, "Only the remainder sizes 2, 4, 8, 16, and 32 are supported");
  DebugAssert(quotient_bits > 0, "quotient size can not be zero.");
  DebugAssert(quotient_bits + remainder_bits <= 64, "The hash length can not exceed 64 bits.");

  _quotient_bits = quotient_bits;
  _remainder_bits = remainder_bits;
  _number_of_slots = std::pow(2, _quotient_bits);
  _hash_bits = _quotient_bits + _remainder_bits;

if (remainder_bits == 2) {
    _quotient_filter2 = gqf2::quotient_filter();
    gqf2::qf_init(&_quotient_filter2.value(), _number_of_slots, _hash_bits, 0);
  } else if (remainder_bits == 4) {
    _quotient_filter4 = gqf4::quotient_filter();
    gqf4::qf_init(&_quotient_filter4.value(), _number_of_slots, _hash_bits, 0);
  } else if (remainder_bits == 8) {
    _quotient_filter8 = gqf8::quotient_filter();
    gqf8::qf_init(&_quotient_filter8.value(), _number_of_slots, _hash_bits, 0);
  } else if (remainder_bits == 16) {
    _quotient_filter16 = gqf16::quotient_filter();
    gqf16::qf_init(&_quotient_filter16.value(), _number_of_slots, _hash_bits, 0);
  } else if (remainder_bits == 32) {
    _quotient_filter32 = gqf32::quotient_filter();
    gqf32::qf_init(&_quotient_filter32.value(), _number_of_slots, _hash_bits, 0);
  }
}

template <typename ElementType>
CountingQuotientFilter<ElementType>::~CountingQuotientFilter() {
  if (_quotient_filter2.has_value()) {
    gqf2::qf_destroy(&_quotient_filter2.value());
  }
  if (_quotient_filter4.has_value()) {
    gqf4::qf_destroy(&_quotient_filter4.value());
  }
  if (_quotient_filter8.has_value()) {
    gqf8::qf_destroy(&_quotient_filter8.value());
  }
  if (_quotient_filter16.has_value()) {
    gqf16::qf_destroy(&_quotient_filter16.value());
  }
  if (_quotient_filter32.has_value()) {
    gqf32::qf_destroy(&_quotient_filter32.value());
  }
}

template <typename ElementType>
void CountingQuotientFilter<ElementType>::insert(ElementType element, uint64_t count) {
  //std::cout << "load factor: " << load_factor() << std::endl;
  uint64_t bitmask = static_cast<uint64_t>(std::pow(2, _hash_bits)) - 1;
  uint64_t hash = bitmask & _hash(element);
  for (uint64_t i = 0; i < count; i++) {
    if (_remainder_bits == 2) {
      gqf2::qf_insert(&_quotient_filter2.value(), hash, 0, 1);
    } else if (_remainder_bits == 4) {
      gqf4::qf_insert(&_quotient_filter4.value(), hash, 0, 1);
    } else if (_remainder_bits == 8) {
      gqf8::qf_insert(&_quotient_filter8.value(), hash, 0, 1);
    } else if (_remainder_bits == 16) {
      gqf16::qf_insert(&_quotient_filter16.value(), hash, 0, 1);
    } else if (_remainder_bits == 32) {
      gqf32::qf_insert(&_quotient_filter32.value(), hash, 0, 1);
    }
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
  if (_remainder_bits == 2) {
    return gqf2::qf_count_key_value(&_quotient_filter2.value(), hash, 0);
  } else if (_remainder_bits == 4) {
    return gqf4::qf_count_key_value(&_quotient_filter4.value(), hash, 0);
  } else if (_remainder_bits == 8) {
    return gqf8::qf_count_key_value(&_quotient_filter8.value(), hash, 0);
  } else if (_remainder_bits == 16) {
    return gqf16::qf_count_key_value(&_quotient_filter16.value(), hash, 0);
  } else {
    return gqf32::qf_count_key_value(&_quotient_filter32.value(), hash, 0);
  }
}

/**
* Computes the hash for a value.
**/
template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::_hash(ElementType value) const {
  auto hash = xxh::xxhash<64, ElementType>(&value, 1, _seed);
  //auto hash = murmur2<ElementType>(value, _seed);
  return static_cast<uint64_t>(hash);
}

/**
* Computes the hash for a string.
**/
template <>
uint64_t CountingQuotientFilter<std::string>::_hash(std::string value) const {
  auto hash = xxh::xxhash<64, char>(value.data(), value.length(), _seed);
  //auto hash = murmur_hash2(value.data(), value.length(), _seed);
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

template <typename ElementType>
uint64_t CountingQuotientFilter<ElementType>::memory_consumption() const {
  uint64_t memory_consumption = 0;
  if (_remainder_bits == 2) {
    memory_consumption += gqf2::memory_consumption(_quotient_filter2.value());
  } else if (_remainder_bits == 4) {
    memory_consumption += gqf4::memory_consumption(_quotient_filter4.value());
  } else if (_remainder_bits == 8) {
    memory_consumption += gqf8::memory_consumption(_quotient_filter8.value());
  } else if (_remainder_bits == 16) {
    memory_consumption += gqf16::memory_consumption(_quotient_filter16.value());
  } else {
    memory_consumption += gqf32::memory_consumption(_quotient_filter32.value());
  }
  //std::cout << "Filter: " << memory_consumption << " bytes" << std::endl;
  return memory_consumption;
}

template <typename ElementType>
double CountingQuotientFilter<ElementType>::load_factor() const {
  if (_remainder_bits == 2) {
    return _quotient_filter2.value().noccupied_slots / static_cast<double>(_quotient_filter2.value().nslots);
  } else if (_remainder_bits == 4) {
    return _quotient_filter4.value().noccupied_slots / static_cast<double>(_quotient_filter4.value().nslots);
  } else if (_remainder_bits == 8) {
    return _quotient_filter8.value().noccupied_slots / static_cast<double>(_quotient_filter8.value().nslots);
  } else if (_remainder_bits == 16) {
    return _quotient_filter16.value().noccupied_slots / static_cast<double>(_quotient_filter16.value().nslots);
  } else {
    return _quotient_filter32.value().noccupied_slots / static_cast<double>(_quotient_filter32.value().nslots);
  }
}

template <typename ElementType>
bool CountingQuotientFilter<ElementType>::is_full() const {
  return load_factor() > 0.99;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(CountingQuotientFilter);

} // namespace opossum
