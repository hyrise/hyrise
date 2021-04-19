#include "murmur_hash.hpp"

#include <cstring>

#define FORCE_INLINE inline __attribute__((always_inline))
#define BIG_CONSTANT(x) (x##LLU)
#define ROTL64(x, y) rotl64(x, y)

namespace opossum {

//-----------------------------------------------------------------------------
// murmur_hash3, by Austin Appleby

// Note - This is the x64 version, on x84 it does _not_ produce the same results, as the
// algorithm is optimized for the x64 platform. You can still
// compile and run it on any platform, but your performance with the
// non-native version will be less than optimal.

inline uint64_t rotl64(uint64_t x, int8_t r) { return (x << r) | (x >> (64 - r)); }

FORCE_INLINE uint64_t getblock64(const uint64_t* p, int i) { return p[i]; }

FORCE_INLINE uint64_t fmix64(uint64_t k) {
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= k >> 33;

  return k;
}

std::pair<uint64_t, uint64_t> murmur_hash3_x64_128(const void* key, const int len, const uint32_t seed) {
  const uint8_t* data = static_cast<const uint8_t*>(key);
  const int nblocks = len / 16;

  uint64_t h1 = seed;
  uint64_t h2 = seed;

  const uint64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
  const uint64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);

  //----------
  // body

  const uint64_t* blocks = reinterpret_cast<const uint64_t*>(data);

  for (int i = 0; i < nblocks; i++) {
    uint64_t k1 = getblock64(blocks, i * 2 + 0);
    uint64_t k2 = getblock64(blocks, i * 2 + 1);

    k1 *= c1;
    k1 = ROTL64(k1, 31);
    k1 *= c2;
    h1 ^= k1;

    h1 = ROTL64(h1, 27);
    h1 += h2;
    h1 = h1 * 5 + 0x52dce729;

    k2 *= c2;
    k2 = ROTL64(k2, 33);
    k2 *= c1;
    h2 ^= k2;

    h2 = ROTL64(h2, 31);
    h2 += h1;
    h2 = h2 * 5 + 0x38495ab5;
  }

  //----------
  // tail

  const uint8_t* tail = reinterpret_cast<const uint8_t*>(data + nblocks * 16);

  uint64_t k1 = 0;
  uint64_t k2 = 0;

  switch (len) {
    case 15:
      k2 ^= (static_cast<uint64_t>(tail[14])) << 48;
      [[fallthrough]];
    case 14:
      k2 ^= (static_cast<uint64_t>(tail[13])) << 40;
      [[fallthrough]];
    case 13:
      k2 ^= (static_cast<uint64_t>(tail[12])) << 32;
      [[fallthrough]];
    case 12:
      k2 ^= (static_cast<uint64_t>(tail[11])) << 24;
      [[fallthrough]];
    case 11:
      k2 ^= (static_cast<uint64_t>(tail[10])) << 16;
      [[fallthrough]];
    case 10:
      k2 ^= (static_cast<uint64_t>(tail[9])) << 8;
      [[fallthrough]];
    case 9:
      k2 ^= (static_cast<uint64_t>(tail[8])) << 0;
      k2 *= c2;
      k2 = ROTL64(k2, 33);
      k2 *= c1;
      h2 ^= k2;
      [[fallthrough]];
    case 8:
      k1 ^= (static_cast<uint64_t>(tail[7])) << 56;
      [[fallthrough]];
    case 7:
      k1 ^= (static_cast<uint64_t>(tail[6])) << 48;
      [[fallthrough]];
    case 6:
      k1 ^= (static_cast<uint64_t>(tail[5])) << 40;
      [[fallthrough]];
    case 5:
      k1 ^= (static_cast<uint64_t>(tail[4])) << 32;
      [[fallthrough]];
    case 4:
      k1 ^= (static_cast<uint64_t>(tail[3])) << 24;
      [[fallthrough]];
    case 3:
      k1 ^= (static_cast<uint64_t>(tail[2])) << 16;
      [[fallthrough]];
    case 2:
      k1 ^= (static_cast<uint64_t>(tail[1])) << 8;
      [[fallthrough]];
    case 1:
      k1 ^= (static_cast<uint64_t>(tail[0])) << 0;
      k1 *= c1;
      k1 = ROTL64(k1, 31);
      k1 *= c2;
      h1 ^= k1;
  }

  //----------
  // finalization

  h1 ^= len;
  h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = fmix64(h1);
  h2 = fmix64(h2);

  h1 += h2;
  h2 += h1;

  // The 128-bit hash is split and stored in two 64-bit unsigned integers
  return std::pair<uint64_t, uint64_t>{h1, h2};
}

//-----------------------------------------------------------------------------
// murmur_hash2, by Austin Appleby

// Note - This code makes a few assumptions about how your machine behaves -

// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4

// And it has a few limitations -

// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.

unsigned int murmur_hash2(const void* key, int len, unsigned int seed) {
  // 'm' and 'r' are mixing constants generated offline.
  // They're not really 'magic', they just happen to work well.

  const unsigned int m = 0x5bd1e995;
  const int r = 24;

  // Initialize the hash to a 'random' value

  unsigned int h = seed ^ len;

  // Mix 4 bytes at a time into the hash
  const unsigned char* data = static_cast<const unsigned char*>(key);

  while (len >= 4) {
    unsigned int k;
    std::memcpy(&k, data, sizeof(k));

    k *= m;
    k ^= k >> r;
    k *= m;

    h *= m;
    h ^= k;

    data += 4;
    len -= 4;
  }

  // Handle the last few bytes of the input array

  switch (len) {
    case 3:
      h ^= data[2] << 16;
      [[fallthrough]];
    case 2:
      h ^= data[1] << 8;
      [[fallthrough]];
    case 1:
      h ^= data[0];
      h *= m;
  }

  // Do a few final mixes of the hash to ensure the last few
  // bytes are well-incorporated.

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
}

}  // namespace opossum