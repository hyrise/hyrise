#pragma once

#include <array>
#include <cstdint>

namespace hyrise {

template <uint8_t FilterSizeExponent, uint8_t K>
class BloomFilter {
 public:
  BloomFilter() {
    _filter.fill(0ULL);
  }

  void insert(uint64_t hash) {
    // std::cout << hash << " -> ";
    for (uint8_t i = 0; i < K; ++i) {
      const auto bit_index = _extract_bits(hash, i);
      _set_bit(bit_index);
    }
    // std::cout << std::endl;
  }

  bool probe(uint64_t hash) const {
    for (uint8_t i = 0; i < K; ++i) {
      uint32_t bit_index = _extract_bits(hash, i);
      if (!_get_bit(bit_index)) {
        return false;
      }
    }
    return true;
  }

  void merge_from(const BloomFilter& other) {
    for (size_t i = 0; i < array_size; ++i) {
      _filter[i] |= other._filter[i];
    }
  }

  double saturation() const {
    uint64_t set_bits = 0;
    for (const auto& word : _filter) {
      set_bits += __builtin_popcountll(word);
    }
    return static_cast<double>(set_bits) / (array_size * 64);
  }

  std::string bit_distribution() const {
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

 protected:
  void _set_bit(uint32_t bit_index) {
    uint32_t array_index = bit_index >> 6;   // bit_index / 64
    uint32_t bit_offset = bit_index & 0x3F;  // bit_index % 64
    _filter[array_index] |= (1ULL << bit_offset);
  }

  bool _get_bit(uint32_t bit_index) const {
    uint32_t array_index = bit_index >> 6;   // bit_index / 64
    uint32_t bit_offset = bit_index & 0x3F;  // bit_index % 64
    return (_filter[array_index] >> bit_offset) & 1ULL;
  }

  uint32_t _extract_bits(uint64_t hash, uint8_t hash_function_index) const {
    uint8_t shift = hash_function_index * FilterSizeExponent;
    return (hash >> shift) & ((1ULL << FilterSizeExponent) - 1);
  }

  // Compile-time validation
  static_assert(FilterSizeExponent >= 6, "FilterSizeExponent must be at least 6 (minimum 64 bits)");
  static_assert(K > 0, "K must be greater than 0");
  static_assert(K * FilterSizeExponent <= 64,
                "Not enough bits in 64-bit hash for K hash functions with this filter size");

  // Array size: 2 ^ FilterSizeExponent bits / 64 bits per uint64_t = 2 ^ (FilterSizeExponent - 6)
  static constexpr auto array_size = 1ULL << (FilterSizeExponent - 6);
  std::array<std::uint64_t, array_size> _filter;
};

template class BloomFilter<16, 1>;
template class BloomFilter<17, 1>;
template class BloomFilter<18, 1>;
template class BloomFilter<19, 1>;
template class BloomFilter<20, 1>;
template class BloomFilter<21, 1>;
template class BloomFilter<22, 1>;
template class BloomFilter<16, 2>;
template class BloomFilter<17, 2>;
template class BloomFilter<18, 2>;
template class BloomFilter<19, 2>;
template class BloomFilter<20, 2>;
template class BloomFilter<21, 2>;
template class BloomFilter<22, 2>;
template class BloomFilter<16, 3>;
template class BloomFilter<17, 3>;
template class BloomFilter<18, 3>;
template class BloomFilter<19, 3>;
template class BloomFilter<20, 3>;
template class BloomFilter<21, 3>;
}  // namespace hyrise