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
  _readonly_filter = reinterpret_cast<uint64_t*>(_filter.data());
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
  _filter[array_index] |= (1ULL << bit_offset);
}

template <uint8_t FilterSizeExponent, uint8_t K>
bool BloomFilter<FilterSizeExponent, K>::_get_bit(uint32_t bit_index) const {
  uint32_t array_index = bit_index >> 6;   // bit_index / 64
  uint32_t bit_offset = bit_index & 0x3F;  // bit_index % 64
  return (_readonly_filter[array_index] >> bit_offset) & 1ULL;
}

template <uint8_t FilterSizeExponent, uint8_t K>
uint32_t BloomFilter<FilterSizeExponent, K>::_extract_bits(uint64_t hash, uint8_t hash_function_index) const {
  uint8_t shift = hash_function_index * FilterSizeExponent;
  return (hash >> shift) & ((1ULL << FilterSizeExponent) - 1);
}

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>::BlockBloomFilter()
    : BaseBloomFilter(FilterSizeExponent, BlockSizeExponent, K) {
  _readonly_filter = reinterpret_cast<uint64_t*>(_filter.data());
}

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
void BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>::insert(uint64_t hash) {
  const auto block_index = (hash >> (size_t{64} - bits_required_for_cacheline_offset)) << 3;
  // const auto& block = &_filter[block_index];
  for (uint8_t i = 0; i < K; ++i) {
    const auto bit_index_in_block = (hash >> i * 9) & 511;
    const auto block_item_index = bit_index_in_block >> 6;  // Index of uint64_t in block
    const auto bit_index_in_item = bit_index_in_block & 63;
    DebugAssert(block_index + block_item_index < _filter.size(), "Calculated index out of range.");
    _filter[block_index + block_item_index] |= (size_t{1} << bit_index_in_item);
  }
}

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
bool BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>::probe(uint64_t hash) const {
  // The upper bits give us the block.
  const auto block_index = (hash >> (size_t{64} - bits_required_for_cacheline_offset)) << 3;
  // const auto& block = &_readonly_filter[block_index];
  auto result = true;
  for (uint8_t i = 0; i < K; ++i) {
    const auto bit_index_in_block = (hash >> i * 9) & size_t{511};
    const auto block_item_index = bit_index_in_block >> 6;  // Index of uint64_t in block
    const auto bit_index_in_item = bit_index_in_block & 63;
    DebugAssert(block_index + block_item_index < _filter.size(), "Calculated index out of range.");

    // std::cout << "Loop result: " << std::boolalpha << result << '\n';
    result &= static_cast<bool>(_readonly_filter[block_index + block_item_index] & (size_t{1} << bit_index_in_item));
    // if (static_cast<bool>(_readonly_filter[block_index + block_item_index] & (size_t{1} << bit_index_in_item))) {
    //   std::cout << "Loop result: " << std::boolalpha << static_cast<bool>(_readonly_filter[block_index + block_item_index] & (size_t{1} << bit_index_in_item)) << '\n';
    //   std::cout << "Hash: " << std::bitset<64>(hash) << ". Block: " << block_index << ". For k " << size_t{i} << ", I want to access bit " << bit_index_in_block << ". That's block item " << block_item_index << " and bit in item: " << bit_index_in_item << "\n";
    //   std::cout << "Loop result: " << std::boolalpha << result << '\n';
    // }
  }
  // std::cout << "Result: " << std::boolalpha << result << '\n';
  return result;
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

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
void BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>::_set_bit(uint32_t bit_index) {
  uint32_t array_index = bit_index >> 6;   // bit_index / 64
  uint32_t bit_offset = bit_index & 0x3F;  // bit_index % 64
  _filter[array_index] |= (1ULL << bit_offset);
}

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
bool BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>::_get_bit(uint32_t bit_index) const {
  uint32_t array_index = bit_index >> 6;   // bit_index / 64
  uint32_t bit_offset = bit_index & 0x3F;  // bit_index % 64
  return (_readonly_filter[array_index] >> bit_offset) & 1ULL;
}

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
uint32_t BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>::_extract_bits(uint64_t hash,
                                                                                   uint8_t hash_function_index) const {
  // Blocked addressing via slicing:
  // - block index: top (FilterSizeExponent - BlockSizeExponent) bits
  // - per-function offset: successive BlockSizeExponent-wide slices from LSB
  constexpr uint32_t block_bits = 1u << BlockSizeExponent;
  constexpr uint32_t block_mask = block_bits - 1u;
  const uint8_t block_index_bits = FilterSizeExponent - BlockSizeExponent;

  uint32_t block_index = 0u;
  if (block_index_bits > 0) {
    const uint64_t blocks_mask = (1ULL << block_index_bits) - 1ULL;
    block_index = static_cast<uint32_t>((hash >> (64 - block_index_bits)) & blocks_mask);
  }

  const uint32_t offset_shift = static_cast<uint32_t>(hash_function_index) * BlockSizeExponent;
  const uint32_t offset = static_cast<uint32_t>((hash >> offset_shift) & block_mask);

  return (block_index << BlockSizeExponent) | offset;
}

template class BloomFilter<16, 1>;
template class BloomFilter<16, 2>;
template class BloomFilter<16, 3>;
template class BloomFilter<17, 1>;
template class BloomFilter<17, 2>;
template class BloomFilter<17, 3>;
template class BloomFilter<18, 1>;
template class BloomFilter<18, 2>;
template class BloomFilter<18, 3>;
template class BloomFilter<19, 1>;
template class BloomFilter<19, 2>;
template class BloomFilter<19, 3>;
template class BloomFilter<20, 1>;
template class BloomFilter<20, 2>;
template class BloomFilter<20, 3>;
template class BloomFilter<21, 1>;
template class BloomFilter<21, 2>;
template class BloomFilter<21, 3>;
template class BloomFilter<22, 1>;
template class BloomFilter<22, 2>;
template class BloomFilter<12, 3>;
template class BloomFilter<23, 1>;
template class BloomFilter<23, 2>;
template class BloomFilter<13, 3>;

// 512-bit blocks (BlockSizeExponent = 9)
template class BlockBloomFilter<18, 8, 1>;
template class BlockBloomFilter<18, 8, 2>;
template class BlockBloomFilter<18, 8, 3>;
template class BlockBloomFilter<19, 8, 1>;
template class BlockBloomFilter<19, 8, 2>;
template class BlockBloomFilter<19, 8, 3>;
template class BlockBloomFilter<20, 8, 1>;
template class BlockBloomFilter<20, 8, 2>;
template class BlockBloomFilter<20, 8, 3>;
template class BlockBloomFilter<21, 8, 1>;
template class BlockBloomFilter<21, 8, 2>;
template class BlockBloomFilter<21, 8, 3>;
template class BlockBloomFilter<22, 8, 1>;
template class BlockBloomFilter<22, 8, 2>;
template class BlockBloomFilter<23, 8, 3>;
template class BlockBloomFilter<23, 8, 1>;
template class BlockBloomFilter<23, 8, 2>;
template class BlockBloomFilter<18, 9, 1>;
template class BlockBloomFilter<18, 9, 2>;
template class BlockBloomFilter<18, 9, 3>;
template class BlockBloomFilter<19, 9, 1>;
template class BlockBloomFilter<19, 9, 2>;
template class BlockBloomFilter<19, 9, 3>;
template class BlockBloomFilter<20, 9, 1>;
template class BlockBloomFilter<20, 9, 2>;
template class BlockBloomFilter<20, 9, 3>;
template class BlockBloomFilter<21, 9, 1>;
template class BlockBloomFilter<21, 9, 2>;
template class BlockBloomFilter<21, 9, 3>;
template class BlockBloomFilter<22, 9, 1>;
template class BlockBloomFilter<22, 9, 2>;
template class BlockBloomFilter<22, 9, 3>;
template class BlockBloomFilter<23, 9, 1>;
template class BlockBloomFilter<23, 9, 2>;
template class BlockBloomFilter<23, 9, 3>;
template class BlockBloomFilter<23, 9, 4>;

}  // namespace hyrise
