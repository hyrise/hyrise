#include "bitpacking_compressor.hpp"
#include "compact_vector.hpp"
#include <algorithm>

namespace opossum {

class BitpackingVector;

template <unsigned T>
pmr_bitpacking_vector<uint32_t, T> BitpackingCompressor::_compressVector(const pmr_vector<uint32_t>& vector, const PolymorphicAllocator<size_t>& alloc, uint32_t b) {
  auto data = pmr_bitpacking_vector<uint32_t, T>(alloc);

  for (int i = 0; i < vector.size(); i++) {
    data.push_back(vector[i]);
  }
  return data;
}

std::unique_ptr<const BaseCompressedVector> BitpackingCompressor::compress(
    const pmr_vector<uint32_t>& vector, const PolymorphicAllocator<size_t>& alloc,
    const UncompressedVectorInfo& meta_info) {
 
  const auto max_value = _find_max_value(vector);
  uint32_t b = std::max(compact::vector<unsigned int, 32>::required_bits(max_value), 1u);

  auto bitVector = std::make_unique<BitpackingVector>();
  
  switch (b) {
    case 0: std::cerr << "can't be zero" << std::endl; break;
    case 1: bitVector.data1 = _compress_vector<1>(); break;
    case 2: bitVector.data2 = _compress_vector<2>(); break;
    case 3: bitVector.data3 = _compress_vector<3>(); break;
    case 4: bitVector.data4 = _compress_vector<4>(); break;
    case 5: bitVector.data5 = _compress_vector<5>(); break;
    case 6: bitVector.data6 = _compress_vector<6>(); break;
    case 7: bitVector.data7 = _compress_vector<7>(); break;
    case 8: bitVector.data8 = _compress_vector<8>(); break;
    case 9: bitVector.data9 = _compress_vector<9>(); break;
    case 10: bitVector.data10 = _compress_vector<10>(); break;
    case 11: bitVector.data11 = _compress_vector<11>(); break;
    case 12: bitVector.data12 = _compress_vector<12>(); break;
    case 13: bitVector.data13 = _compress_vector<13>(); break;
    case 14: bitVector.data14 = _compress_vector<14>(); break;
    case 15: bitVector.data15 = _compress_vector<15>(); break;
    case 16: bitVector.data16 = _compress_vector<16>(); break;
    case 17: bitVector.data17 = _compress_vector<17>(); break;
    case 18: bitVector.data18 = _compress_vector<18>(); break;
    case 19: bitVector.data19 = _compress_vector<19>(); break;
    case 20: bitVector.data20 = _compress_vector<20>(); break;
    case 21: bitVector.data21 = _compress_vector<21>(); break;
    case 22: bitVector.data22 = _compress_vector<22>(); break;
    case 23: bitVector.data23 = _compress_vector<23>(); break;
    case 24: bitVector.data24 = _compress_vector<24>(); break;
    case 25: bitVector.data25 = _compress_vector<25>(); break;
    case 26: bitVector.data26 = _compress_vector<26>(); break;
    case 27: bitVector.data27 = _compress_vector<27>(); break;
    case 28: bitVector.data28 = _compress_vector<28>(); break;
    case 29: bitVector.data29 = _compress_vector<29>(); break;
    case 30: bitVector.data30 = _compress_vector<30>(); break;
    case 31: bitVector.data31 = _compress_vector<31>(); break;
    case 32: bitVector.data32 = _compress_vector<32>(); break;
    default: std::cerr << "unexpected b=" << b << std::endl; break;
  }
  bitVector.b = b;

  return bitVector;

}

std::unique_ptr<BaseVectorCompressor> BitpackingCompressor::create_new() const {
  return std::make_unique<BitpackingCompressor>();
}

uint32_t BitpackingCompressor::_find_max_value(const pmr_vector<uint32_t>& vector) const {
  uint32_t max = 0;
  for (const auto v : vector) {
    max |= v;
  }
  return max;
}


}  // namespace opossum
