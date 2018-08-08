#include "murmur_hash.hpp"

#include <cstring>

namespace opossum {

//-----------------------------------------------------------------------------
// murmur_hash2, by Austin Appleby

// Note - This code makes a few assumptions about how your machine behaves -

// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4

// And it has a few limitations -

// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.

unsigned int murmur_hash2(const void* key, unsigned int len, unsigned int seed) {
  // 'm' and 'r' are mixing constants generated offline.
  // They're not really 'magic', they just happen to work well.

  const unsigned int m = 0x5bd1e995;
  const unsigned int r = 24;

  // Initialize the hash to a 'random' value

  unsigned int h = seed ^ len;

  // Mix 4 bytes at a time into the hash

  const auto* data = static_cast<const unsigned char*>(key);

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
      h ^= data[2] << 16u;
      [[fallthrough]];
    case 2:
      h ^= data[1] << 8u;
      [[fallthrough]];
    case 1:
      h ^= data[0];
      h *= m;
  }

  // Do a few final mixes of the hash to ensure the last few
  // bytes are well-incorporated.

  h ^= h >> 13u;
  h *= m;
  h ^= h >> 15u;

  return h;
}

}  // namespace opossum
