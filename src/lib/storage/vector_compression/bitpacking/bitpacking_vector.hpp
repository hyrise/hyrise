#pragma once

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "compact_vector.hpp"

#include "vector_types.hpp"

#include "bitpacking_iterator.hpp"
#include "bitpacking_decompressor.hpp"

namespace opossum {

class BitpackingDecompressor;
class BitpackingIterator; 

class BitpackingVector : public CompressedVector<BitpackingVector> {
 public:
  explicit BitpackingVector();

  ~BitpackingVector() override = default;

  size_t on_size() const;
  size_t on_data_size() const;

  std::unique_ptr<BaseVectorDecompressor> on_create_base_decompressor() const;
  BitpackingDecompressor on_create_decompressor() const;

  BitpackingIterator on_begin() const;
  BitpackingIterator on_end() const;

  std::unique_ptr<const BaseCompressedVector> on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const;

 private:

  template <unsigned T>
  pmr_bitpacking_vector<uint32_t, T> _copyVector(pmr_bitpacking_vector<uint32_t, T> data);

  friend class BitpackingDecompressor;

public:

  const int b;

  const pmr_bitpacking_vector<uint32_t, 1> data1;
  const pmr_bitpacking_vector<uint32_t, 2> data2;
  const pmr_bitpacking_vector<uint32_t, 3> data3;
  const pmr_bitpacking_vector<uint32_t, 4> data4;
  const pmr_bitpacking_vector<uint32_t, 5> data5;
  const pmr_bitpacking_vector<uint32_t, 6> data6;
  const pmr_bitpacking_vector<uint32_t, 7> data7;
  const pmr_bitpacking_vector<uint32_t, 8> data8;
  const pmr_bitpacking_vector<uint32_t, 9> data9;
  const pmr_bitpacking_vector<uint32_t, 10> data10;
  const pmr_bitpacking_vector<uint32_t, 11> data11;
  const pmr_bitpacking_vector<uint32_t, 12> data12;
  const pmr_bitpacking_vector<uint32_t, 13> data13;
  const pmr_bitpacking_vector<uint32_t, 14> data14;
  const pmr_bitpacking_vector<uint32_t, 15> data15;
  const pmr_bitpacking_vector<uint32_t, 16> data16;
  const pmr_bitpacking_vector<uint32_t, 17> data17;
  const pmr_bitpacking_vector<uint32_t, 18> data18;
  const pmr_bitpacking_vector<uint32_t, 19> data19;
  const pmr_bitpacking_vector<uint32_t, 20> data20;
  const pmr_bitpacking_vector<uint32_t, 21> data21;
  const pmr_bitpacking_vector<uint32_t, 22> data22;
  const pmr_bitpacking_vector<uint32_t, 23> data23;
  const pmr_bitpacking_vector<uint32_t, 24> data24;
  const pmr_bitpacking_vector<uint32_t, 25> data25;
  const pmr_bitpacking_vector<uint32_t, 26> data26;
  const pmr_bitpacking_vector<uint32_t, 27> data27;
  const pmr_bitpacking_vector<uint32_t, 28> data28;
  const pmr_bitpacking_vector<uint32_t, 29> data29;
  const pmr_bitpacking_vector<uint32_t, 30> data30;
  const pmr_bitpacking_vector<uint32_t, 31> data31;
  const pmr_bitpacking_vector<uint32_t, 32> data32;

  uint32_t get(size_t i) {
    switch (b) {
      case 0: std::cerr << "can't be zero" << std::endl; break;
      case 1: return data1[i];
      case 2: return data2[i];
      case 3: return data3[i];
      case 4: return data4[i];
      case 5: return data5[i];
      case 6: return data6[i];
      case 7: return data7[i];
      case 8: return data8[i];
      case 9: return data9[i];
      case 10: return data10[i];
      case 11: return data11[i];
      case 12: return data12[i];
      case 13: return data13[i];
      case 14: return data14[i];
      case 15: return data15[i];
      case 16: return data16[i];
      case 17: return data17[i];
      case 18: return data18[i];
      case 19: return data19[i];
      case 20: return data20[i];
      case 21: return data21[i];
      case 22: return data22[i];
      case 23: return data23[i];
      case 24: return data24[i];
      case 25: return data25[i];
      case 26: return data26[i];
      case 27: return data27[i];
      case 28: return data28[i];
      case 29: return data29[i];
      case 30: return data30[i];
      case 31: return data31[i];
      case 32: return data32[i];
      default: std::cerr << "unexpected b=" << b << std::endl; break;
    }
  }
};

}  // namespace opossum
