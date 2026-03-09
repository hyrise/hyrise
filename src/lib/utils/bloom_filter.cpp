#include "bloom_filter.hpp"

#include <array>
#include <atomic>
#include <bitset>
#include <cstdint>

namespace hyrise {

BaseBloomFilter::BaseBloomFilter(const uint8_t filter_size_exponent, const uint8_t block_size_exponent, const uint8_t k)
    : _filter_size_exponent(filter_size_exponent), _block_size_exponent(block_size_exponent), _k(k) {}

uint8_t BaseBloomFilter::filter_size_exponent() const {
  return _filter_size_exponent;
}

uint8_t BaseBloomFilter::block_size_exponent() const {
  return _block_size_exponent;
}

uint8_t BaseBloomFilter::k() const {
  return _k;
}

template <uint8_t FilterSizeExponent, uint8_t K>
BloomFilter<FilterSizeExponent, K>::BloomFilter() : BaseBloomFilter(FilterSizeExponent, 0, K) {
  _integer_filter_view = reinterpret_cast<uint64_t*>(_filter.data());
}

template <uint8_t FilterSizeExponent, uint8_t K>
void BloomFilter<FilterSizeExponent, K>::insert(uint64_t hash) {
  for (uint8_t i = 0; i < K; ++i) {
    const auto bit_index = _extract_bits(hash, i);
    _set_bit(bit_index);
  }
}

template <uint8_t FilterSizeExponent, uint8_t K>
bool BloomFilter<FilterSizeExponent, K>::probe(uint64_t hash) const {
  auto result = true;
  for (uint8_t i = 0; i < K; ++i) {
    uint32_t bit_index = _extract_bits(hash, i);
    result &= _get_bit(bit_index);
  }
  return result;
}

template <uint8_t FilterSizeExponent, uint8_t K>
void BloomFilter<FilterSizeExponent, K>::merge_from(const BaseBloomFilter& other) {
  const auto* typed_other = dynamic_cast<const BloomFilter<FilterSizeExponent, K>*>(&other);
  if (!typed_other) {
    throw std::invalid_argument("Incompatible BloomFilter types for merge");
  }

  for (size_t i = 0; i < array_size; ++i) {
    const uint64_t other_word = typed_other->_filter[i].load(std::memory_order_acquire);
    _filter[i].fetch_or(other_word, std::memory_order_acq_rel);
  }
}

template <uint8_t FilterSizeExponent, uint8_t K>
double BloomFilter<FilterSizeExponent, K>::saturation() const {
  uint64_t set_bits = 0;
  for (const auto& word : _filter) {
    set_bits += __builtin_popcountll(word);
  }
  return static_cast<double>(set_bits) / (array_size * 64);
}

template <uint8_t FilterSizeExponent, uint8_t K>
std::string BloomFilter<FilterSizeExponent, K>::bit_distribution() const {
  std::array<uint32_t, 100> distribution{};
  constexpr uint32_t bits_per_bucket = (array_size * 64) / 100;

  for (uint32_t i = 0; i < array_size * 64; ++i) {
    uint32_t array_index = i >> 6;   // i / 64
    uint32_t bit_offset = i & 0x3F;  // i % 64
    if ((_filter[array_index] >> bit_offset) & 1ULL) {
      uint32_t bucket_index = i / bits_per_bucket;
      if (bucket_index == 100) {
        bucket_index = 99;  // Ensure we don't go out of bounds
      }
      Assert(bucket_index < 100, "Bucket index out of range");
      ++distribution[bucket_index];
    }
  }

  std::string csv_output;
  for (size_t i = 0; i < 100; ++i) {
    csv_output += std::to_string(distribution[i]);
    if (i < 99) {
      csv_output += ":";
    }
  }
  return csv_output;
}

template <uint8_t FilterSizeExponent, uint8_t K>
void BloomFilter<FilterSizeExponent, K>::_set_bit(uint32_t bit_index) {
  uint32_t array_index = bit_index >> 6;   // bit_index / 64
  uint32_t bit_offset = bit_index & 0x3F;  // bit_index % 64
  _integer_filter_view[array_index] |= (1ULL << bit_offset);
}

template <uint8_t FilterSizeExponent, uint8_t K>
bool BloomFilter<FilterSizeExponent, K>::_get_bit(uint32_t bit_index) const {
  uint32_t array_index = bit_index >> 6;   // bit_index / 64
  uint32_t bit_offset = bit_index & 0x3F;  // bit_index % 64
  return (_integer_filter_view[array_index] >> bit_offset) & 1ULL;
}

template <uint8_t FilterSizeExponent, uint8_t K>
uint32_t BloomFilter<FilterSizeExponent, K>::_extract_bits(uint64_t hash, uint8_t hash_function_index) const {
  uint8_t shift = hash_function_index * FilterSizeExponent;
  return (hash >> shift) & ((1ULL << FilterSizeExponent) - 1);
}

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>::BlockBloomFilter()
    : BaseBloomFilter(FilterSizeExponent, BlockSizeExponent, K) {
  _integer_filter_view = reinterpret_cast<uint64_t*>(_filter.data());
}

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
void BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>::merge_from(const BaseBloomFilter& other) {
  const auto* typed_other = dynamic_cast<const BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>*>(&other);
  if (!typed_other) {
    throw std::invalid_argument("Incompatible BlockBloomFilter types for merge");
  }

  for (size_t i = 0; i < array_size; ++i) {
    const uint64_t other_word = typed_other->_filter[i].load(std::memory_order_acquire);
    _filter[i].fetch_or(other_word, std::memory_order_acq_rel);
  }
}

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
double BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>::saturation() const {
  uint64_t set_bits = 0;
  for (const auto& word : _filter) {
    set_bits += __builtin_popcountll(word);
  }
  return static_cast<double>(set_bits) / (array_size * 64);
}

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
std::string BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>::bit_distribution() const {
  std::array<uint32_t, 100> distribution{};
  constexpr uint32_t bits_per_bucket = (array_size * 64) / 100;

  for (uint32_t i = 0; i < array_size * 64; ++i) {
    uint32_t array_index = i >> 6;   // i / 64
    uint32_t bit_offset = i & 0x3F;  // i % 64
    if ((_filter[array_index] >> bit_offset) & 1ULL) {
      uint32_t bucket_index = i / bits_per_bucket;
      if (bucket_index == 100) {
        bucket_index = 99;  // Ensure we don't go out of bounds
      }
      Assert(bucket_index < 100, "Bucket index out of range");
      ++distribution[bucket_index];
    }
  }

  std::string csv_output;
  for (size_t i = 0; i < 100; ++i) {
    csv_output += std::to_string(distribution[i]);
    if (i < 99) {
      csv_output += ":";
    }
  }
  return csv_output;
}

template class BlockBloomFilter<18, 9, 1>;
template class BlockBloomFilter<18, 9, 2>;
template class BlockBloomFilter<18, 9, 3>;
template class BlockBloomFilter<18, 9, 4>;
template class BlockBloomFilter<21, 9, 1>;
template class BlockBloomFilter<21, 9, 2>;
template class BlockBloomFilter<21, 9, 3>;
template class BlockBloomFilter<21, 9, 4>;
template class BlockBloomFilter<23, 9, 1>;
template class BlockBloomFilter<23, 9, 2>;
template class BlockBloomFilter<23, 9, 3>;
template class BlockBloomFilter<23, 9, 4>;

}  // namespace hyrise
