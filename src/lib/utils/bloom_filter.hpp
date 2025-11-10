#pragma once

#include <array>
#include <atomic>
#include <bitset>
#include <cstdint>

namespace hyrise {

class BaseBloomFilter {
 public:
  BaseBloomFilter(const uint8_t filter_size_exponent, const uint8_t block_size_exponent, const uint8_t k)
      : _filter_size_exponent(filter_size_exponent), _block_size_exponent(block_size_exponent), _k(k) {}

  virtual ~BaseBloomFilter() = default;

  virtual void merge_from(const BaseBloomFilter& other) = 0;
  virtual double saturation() const = 0;
  virtual std::string bit_distribution() const = 0;

  uint8_t filter_size_exponent() const {
    return _filter_size_exponent;
  }

  uint8_t block_size_exponent() const {
    return _block_size_exponent;
  }

  uint8_t k() const {
    return _k;
  }

  bool is_dummy() const {
    return _k == 0;
  }

 private:
  const uint8_t _filter_size_exponent;
  const uint8_t _block_size_exponent;
  const uint8_t _k;
};

template <uint8_t FilterSizeExponent, uint8_t K>
class BloomFilter : public BaseBloomFilter {
 public:
  BloomFilter() : BaseBloomFilter(FilterSizeExponent, 0, K) {
    _readonly_filter = reinterpret_cast<uint64_t*>(_filter.data());
  }

  void insert(uint64_t hash) {
    for (uint8_t i = 0; i < K; ++i) {
      const auto bit_index = _extract_bits(hash, i);
      _set_bit(bit_index);
    }
  }

  bool probe(uint64_t hash) const {
    // if (1 == 1) return false;
    for (uint8_t i = 0; i < K; ++i) {
      uint32_t bit_index = _extract_bits(hash, i);
      if (!_get_bit(bit_index)) {
        return false;
      }
    }
    return true;
  }

  void merge_from(const BaseBloomFilter& other) override final {
    const auto* typed_other = dynamic_cast<const BloomFilter<FilterSizeExponent, K>*>(&other);
    if (!typed_other) {
      throw std::invalid_argument("Incompatible BloomFilter types for merge");
    }

    for (size_t i = 0; i < array_size; ++i) {
      const uint64_t other_word = typed_other->_filter[i].load(std::memory_order_acquire);
      _filter[i].fetch_or(other_word, std::memory_order_acq_rel);
    }
  }

  double saturation() const override final {
    uint64_t set_bits = 0;
    for (const auto& word : _filter) {
      set_bits += __builtin_popcountll(word);
    }
    return static_cast<double>(set_bits) / (array_size * 64);
  }

  std::string bit_distribution() const override final {
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
    return (_readonly_filter[array_index] >> bit_offset) & 1ULL;
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
  alignas(64) std::array<std::atomic<uint64_t>, array_size> _filter;
  uint64_t* _readonly_filter;
};

template <uint8_t FilterSizeExponent, uint8_t BlockSizeExponent, uint8_t K>
class BlockBloomFilter : public BaseBloomFilter {
 public:
  BlockBloomFilter() : BaseBloomFilter(FilterSizeExponent, BlockSizeExponent, K) {
    _readonly_filter = reinterpret_cast<uint64_t*>(_filter.data());
  }

  void insert(uint64_t hash) {
    auto ss = std::stringstream{};
    const auto block_index = hash >> (64 - bits_required_for_block_offset);
    // const auto& block = &_filter[block_index];
    for (uint8_t i = 0; i < K; ++i) {
      const auto bit_index_in_block = (hash >> i * 9) & 511;
      const auto block_item_index = bit_index_in_block >> 6;  // Index of uint64_t in block
      const auto bit_index_in_item = bit_index_in_block & 63;
      _filter[block_index + block_item_index] |= (size_t{1} << bit_index_in_item);
      // std::cout << "Hash: " << std::bitset<64>(hash) << ". Added item to block: " << block_index << ". For k " << size_t{i} << ", I want to access bit " << bit_index_in_block << ". That's block item " << block_item_index << " and bit in item: " << bit_index_in_item << "\n";
      // std::cout << "uint64_t afterwards: " << std::bitset<64>(_filter[block_index + block_item_index]) << '\n';
      // std::cout << "Test print : " << std::bitset<64>(size_t{1} << bit_index_in_item) << '\n';
    }
  }

  bool probe(uint64_t hash) const {
    // The upper bits give us the block.
    const auto block_index = hash >> (64 - bits_required_for_block_offset);
    // const auto& block = &_readonly_filter[block_index];
    auto result = true;
    for (uint8_t i = 0; i < K; ++i) {
      const auto bit_index_in_block = (hash >> i * 9) & size_t{511};
      const auto block_item_index = bit_index_in_block >> 6;  // Index of uint64_t in block
      const auto bit_index_in_item = bit_index_in_block & 63;

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

  void merge_from(const BaseBloomFilter& other) override final {
    const auto* typed_other = dynamic_cast<const BlockBloomFilter<FilterSizeExponent, BlockSizeExponent, K>*>(&other);
    if (!typed_other) {
      throw std::invalid_argument("Incompatible BlockBloomFilter types for merge");
    }

    for (size_t i = 0; i < array_size; ++i) {
      const uint64_t other_word = typed_other->_filter[i].load(std::memory_order_acquire);
      _filter[i].fetch_or(other_word, std::memory_order_acq_rel);
    }
  }

  double saturation() const override final {
    uint64_t set_bits = 0;
    for (const auto& word : _filter) {
      set_bits += __builtin_popcountll(word);
    }
    return static_cast<double>(set_bits) / (array_size * 64);
  }

  std::string bit_distribution() const override final {
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
    return (_readonly_filter[array_index] >> bit_offset) & 1ULL;
  }

  uint32_t _extract_bits(uint64_t hash, uint8_t hash_function_index) const {
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

  // Compile-time validation
  static_assert(FilterSizeExponent >= 6, "FilterSizeExponent must be at least 6 (minimum 64 bits)");
  static_assert(K > 0, "K must be greater than 0");
  // Ensure blocked slicing has enough bits in the 64-bit hash:
  static_assert(BlockSizeExponent <= FilterSizeExponent, "BlockSizeExponent must be <= FilterSizeExponent");
  static_assert((FilterSizeExponent - BlockSizeExponent) + K * BlockSizeExponent <= 64,
                "Not enough bits for block index plus K offsets of size BlockSizeExponent");

  // Array size: 2 ^ FilterSizeExponent bits / 64 bits per uint64_t = 2 ^ (FilterSizeExponent - 6)
  static constexpr auto array_size = 1ULL << (FilterSizeExponent - 6);
  static constexpr auto bits_required_for_block_offset = FilterSizeExponent - 6;
  alignas(64) std::array<std::atomic<uint64_t>, array_size> _filter;
  uint64_t* _readonly_filter;
};

// template class BloomFilter<16, 1>;
// template class BloomFilter<17, 1>;
// template class BloomFilter<18, 1>;
// template class BloomFilter<19, 1>;
template class BloomFilter<20, 1>;
// template class BloomFilter<21, 1>;
// template class BloomFilter<22, 1>;
// template class BloomFilter<16, 2>;
// template class BloomFilter<17, 2>;
// template class BloomFilter<18, 2>;
// template class BloomFilter<19, 2>;
template class BloomFilter<20, 2>;
// template class BloomFilter<21, 2>;
// template class BloomFilter<22, 2>;
// template class BloomFilter<16, 3>;
// template class BloomFilter<17, 3>;
// template class BloomFilter<18, 3>;
// template class BloomFilter<19, 3>;
// template class BloomFilter<20, 3>;
// template class BloomFilter<21, 3>;

// 512-bit blocks (BlockSizeExponent = 9)
// template class BlockBloomFilter<16, 9, 1>;
// template class BlockBloomFilter<17, 9, 1>;
// template class BlockBloomFilter<18, 9, 1>;
// template class BlockBloomFilter<19, 9, 1>;
template class BlockBloomFilter<20, 9, 1>;
// template class BlockBloomFilter<21, 9, 1>;
// template class BlockBloomFilter<22, 9, 1>;
// template class BlockBloomFilter<16, 9, 2>;
// template class BlockBloomFilter<17, 9, 2>;
// template class BlockBloomFilter<18, 9, 2>;
// template class BlockBloomFilter<19, 9, 2>;
template class BlockBloomFilter<20, 9, 2>;

// template class BlockBloomFilter<21, 9, 2>;
// template class BlockBloomFilter<22, 9, 2>;
// template class BlockBloomFilter<16, 9, 3>;
// template class BlockBloomFilter<17, 9, 3>;
// template class BlockBloomFilter<18, 9, 3>;
// template class BlockBloomFilter<19, 9, 3>;
// template class BlockBloomFilter<20, 9, 3>;
// template class BlockBloomFilter<21, 9, 3>;

template <typename Functor>
void resolve_bloom_filter_type(BaseBloomFilter& base_bloom_filter, const Functor& functor) {
  switch (base_bloom_filter.filter_size_exponent()) {
    case 20: {
      switch (base_bloom_filter.block_size_exponent()) {
        case 0: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BloomFilter<20, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BloomFilter<20, 2>&>(base_bloom_filter));
              break;
            default:
              Fail("Unsupported bloom filter type.");
          }
        } break;
        case 8: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BlockBloomFilter<20, 8, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BlockBloomFilter<20, 8, 2>&>(base_bloom_filter));
              break;
            default:
              Fail("Unsupported bloom filter type.");
          }
        } break;
        case 9: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BlockBloomFilter<20, 9, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BlockBloomFilter<20, 9, 2>&>(base_bloom_filter));
              break;
            default:
              Fail("Unsupported bloom filter type.");
          }
        } break;
        default:
          Fail("Unsupported bloom filter type.");
      }
    } break;
    default:
      Fail("Unsupported bloom filter type.");
  }
}

}  // namespace hyrise
