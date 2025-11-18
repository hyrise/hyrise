#pragma once

#include <array>
#include <atomic>
#include <bitset>
#include <cstdint>

namespace hyrise {

class BaseBloomFilter {
 public:
  BaseBloomFilter(const uint8_t filter_size_exponent, const uint8_t block_size_exponent, const uint8_t k);

  virtual ~BaseBloomFilter() = default;

  virtual void merge_from(const BaseBloomFilter& other) = 0;
  virtual double saturation() const = 0;
  virtual std::string bit_distribution() const = 0;

  uint8_t filter_size_exponent() const;

  uint8_t block_size_exponent() const;

  uint8_t k() const;

 private:
  const uint8_t _filter_size_exponent;
  const uint8_t _block_size_exponent;
  const uint8_t _k;
};

template <uint8_t FilterSizeExponent, uint8_t K>
class BloomFilter : public BaseBloomFilter {
 public:
  BloomFilter();

  void insert(uint64_t hash);

  bool probe(uint64_t hash) const;

  void merge_from(const BaseBloomFilter& other) override final;

  double saturation() const override final;

  std::string bit_distribution() const override final;

 protected:
  void _set_bit(uint32_t bit_index);

  bool _get_bit(uint32_t bit_index) const;

  uint32_t _extract_bits(uint64_t hash, uint8_t hash_function_index) const;

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
  BlockBloomFilter();

  void insert(uint64_t hash);

  bool probe(uint64_t hash) const;

  void merge_from(const BaseBloomFilter& other) override final;

  double saturation() const override final;

  std::string bit_distribution() const override final;

 protected:
  void _set_bit(uint32_t bit_index);

  bool _get_bit(uint32_t bit_index) const;

  uint32_t _extract_bits(uint64_t hash, uint8_t hash_function_index) const;

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
  static constexpr auto bits_required_for_cacheline_offset = bits_required_for_block_offset - 3;
  alignas(64) std::array<std::atomic<uint64_t>, array_size> _filter;
  uint64_t* _readonly_filter;
};

template <typename Functor>
void resolve_bloom_filter_type(BaseBloomFilter& base_bloom_filter, const Functor& functor) {
  switch (base_bloom_filter.filter_size_exponent()) {
    case 18: {
      switch (base_bloom_filter.block_size_exponent()) {
        case 0: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BloomFilter<18, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BloomFilter<18, 2>&>(base_bloom_filter));
              break;
            default:
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
              Fail("Unsupported bloom filter type.");
          }
        } break;
        case 8: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BlockBloomFilter<18, 8, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BlockBloomFilter<18, 8, 2>&>(base_bloom_filter));
              break;
            default:
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
              Fail("Unsupported bloom filter type.");
          }
        } break;
        case 9: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BlockBloomFilter<18, 9, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BlockBloomFilter<18, 9, 2>&>(base_bloom_filter));
              break;
            case 3:
              functor(static_cast<BlockBloomFilter<18, 9, 3>&>(base_bloom_filter));
              break;
            default:
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
              Fail("Unsupported bloom filter type.");
          }
        } break;
        default:
          std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                    << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                    << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
          Fail("Unsupported bloom filter type.");
      }
    } break;
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
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
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
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
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
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
              Fail("Unsupported bloom filter type.");
          }
        } break;
        default:
          std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                    << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                    << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
          Fail("Unsupported bloom filter type.");
      }
    } break;
    case 21: {
      switch (base_bloom_filter.block_size_exponent()) {
        case 0: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BloomFilter<21, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BloomFilter<21, 2>&>(base_bloom_filter));
              break;
            default:
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
              Fail("Unsupported bloom filter type.");
          }
        } break;
        case 8: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BlockBloomFilter<21, 8, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BlockBloomFilter<21, 8, 2>&>(base_bloom_filter));
              break;
            default:
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
              Fail("Unsupported bloom filter type.");
          }
        } break;
        case 9: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BlockBloomFilter<21, 9, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BlockBloomFilter<21, 9, 2>&>(base_bloom_filter));
              break;
            case 3:
              functor(static_cast<BlockBloomFilter<21, 9, 3>&>(base_bloom_filter));
              break;
            default:
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
              Fail("Unsupported bloom filter type.");
          }
        } break;
        default:
          std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                    << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                    << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
          Fail("Unsupported bloom filter type.");
      }
    } break;
    case 23: {
      switch (base_bloom_filter.block_size_exponent()) {
        case 0: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BloomFilter<23, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BloomFilter<23, 2>&>(base_bloom_filter));
              break;
            default:
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
              Fail("Unsupported bloom filter type.");
          }
        } break;
        case 8: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BlockBloomFilter<23, 8, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BlockBloomFilter<23, 8, 2>&>(base_bloom_filter));
              break;
            default:
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
              Fail("Unsupported bloom filter type.");
          }
        } break;
        case 9: {
          switch (base_bloom_filter.k()) {
            case 1:
              functor(static_cast<BlockBloomFilter<23, 9, 1>&>(base_bloom_filter));
              break;
            case 2:
              functor(static_cast<BlockBloomFilter<23, 9, 2>&>(base_bloom_filter));
              break;
            case 3:
              functor(static_cast<BlockBloomFilter<23, 9, 3>&>(base_bloom_filter));
              break;
            default:
              std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                        << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                        << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
              Fail("Unsupported bloom filter type.");
          }
        } break;
        default:
          std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                    << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                    << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
          Fail("Unsupported bloom filter type.");
      }
    } break;
    default:
      std::cout << "Failed exponent: " << static_cast<int>(base_bloom_filter.filter_size_exponent())
                << ", block exponent: " << static_cast<int>(base_bloom_filter.block_size_exponent())
                << ", k: " << static_cast<int>(base_bloom_filter.k()) << std::endl;
      Fail("Unsupported bloom filter type.");
  }
}

}  // namespace hyrise
