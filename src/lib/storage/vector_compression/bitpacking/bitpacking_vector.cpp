#include "bitpacking_vector.hpp"

namespace opossum {

BitpackingVector::BitpackingVector() : {}

size_t BitpackingVector::on_size() const { 
  switch (b) {
      case 0: std::cerr << "can't be zero" << std::endl; break;
      case 1: return data1.size();
      case 2: return data2.size();
      case 3: return data3.size();
      case 4: return data4.size();
      case 5: return data5.size();
      case 6: return data6.size();
      case 7: return data7.size();
      case 8: return data8.size();
      case 9: return data9.size();
      case 10: return data10.size();
      case 11: return data11.size();
      case 12: return data12.size();
      case 13: return data13.size();
      case 14: return data14.size();
      case 15: return data15.size();
      case 16: return data16.size();
      case 17: return data17.size();
      case 18: return data18.size();
      case 19: return data19.size();
      case 20: return data20.size();
      case 21: return data21.size();
      case 22: return data22.size();
      case 23: return data23.size();
      case 24: return data24.size();
      case 25: return data25.size();
      case 26: return data26.size();
      case 27: return data27.size();
      case 28: return data28.size();
      case 29: return data29.size();
      case 30: return data30.size();
      case 31: return data31.size();
      case 32: return data32.size();
      default: std::cerr << "unexpected b=" << b << std::endl; break;
}
size_t BitpackingVector::on_data_size() const {
  switch (b) {
    case 0: std::cerr << "can't be zero" << std::endl; break;
    case 1: return data1.bytes();
    case 2: return data2.bytes();
    case 3: return data3.bytes();
    case 4: return data4.bytes();
    case 5: return data5.bytes();
    case 6: return data6.bytes();
    case 7: return data7.bytes();
    case 8: return data8.bytes();
    case 9: return data9.bytes();
    case 10: return data10.bytes();
    case 11: return data11.bytes();
    case 12: return data12.bytes();
    case 13: return data13.bytes();
    case 14: return data14.bytes();
    case 15: return data15.bytes();
    case 16: return data16.bytes();
    case 17: return data17.bytes();
    case 18: return data18.bytes();
    case 19: return data19.bytes();
    case 20: return data20.bytes();
    case 21: return data21.bytes();
    case 22: return data22.bytes();
    case 23: return data23.bytes();
    case 24: return data24.bytes();
    case 25: return data25.bytes();
    case 26: return data26.bytes();
    case 27: return data27.bytes();
    case 28: return data28.bytes();
    case 29: return data29.bytes();
    case 30: return data30.bytes();
    case 31: return data31.bytes();
    case 32: return data32.bytes();
    default: std::cerr << "unexpected b=" << b << std::endl; break;
  }
}

std::unique_ptr<BaseVectorDecompressor> BitpackingVector::on_create_base_decompressor() const {
  return std::make_unique<BitpackingDecompressor>(*this);
}

BitpackingDecompressor BitpackingVector::on_create_decompressor() const { 
  prepareGet();
  return BitpackingDecompressor(*this); 
}

BitpackingIterator BitpackingVector::on_begin() const { return BitpackingIterator(*this, 0u);}

BitpackingIterator BitpackingVector::on_end() const { return BitpackingIterator(*this, on_size()); }

template <unsigned T>
pmr_bitpacking_vector<uint32_t, T> BitpackingVector::_copyVector(pmr_bitpacking_vector<uint32_t, T> data) {
  auto data_copy = pmr_bitpacking_vector<uint32_t, T>(data.used_bits(), data.size(), alloc);
  for (int i = 0; i < data.size(); i++) {
    data_copy[i] = data[i];
  }
  return data_copy;
}

std::unique_ptr<const BaseCompressedVector> BitpackingVector::on_copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  
  auto bv = std::make_unique<BitpackingVector>();
  bv.b = b;

  switch (b) {
    case 0: std::cerr << "can't be zero" << std::endl; break;
    case 1: {
      if (data1.size() > 0) {
        auto data1_copy = _copyVector<1>(data1);
        bv.data1 = data1_copy;
      } 
      break;
    }
    case 2: {
      if (data2.size() > 0) {
        auto data2_copy = _copyVector<2>(data2);
        bv.data2 = data2_copy;
      } 
      break;
    }
    case 3: {
      if (data3.size() > 0) {
        auto data3_copy = _copyVector<3>(data3);
        bv.data3 = data3_copy;
      } 
      break;
    }
    case 4: {
      if (data4.size() > 0) {
        auto data4_copy = _copyVector<4>(data4);
        bv.data4 = data4_copy;
      } 
      break;
    }
    case 5: {
      if (data5.size() > 0) {
        auto data5_copy = _copyVector<5>(data5);
        bv.data5 = data5_copy;
      } 
      break;
    }
    case 6: {
      if (data6.size() > 0) {
        auto data6_copy = _copyVector<6>(data6);
        bv.data6 = data6_copy;
      } 
      break;
    }
    case 7: {
      if (data7.size() > 0) {
        auto data7_copy = _copyVector<7>(data7);
        bv.data7 = data7_copy;
      } 
      break;
    }
    case 8: {
      if (data8.size() > 0) {
        auto data8_copy = _copyVector<8>(data8);
        bv.data8 = data8_copy;
      } 
      break;
    }
    case 9: {
      if (data9.size() > 0) {
        auto data9_copy = _copyVector<9>(data9);
        bv.data9 = data9_copy;
      } 
      break;
    }
    case 10: {
      if (data10.size() > 0) {
        auto data10_copy = _copyVector<10>(data10);
        bv.data10 = data10_copy;
      } 
      break;
    }
    case 11: {
      if (data11.size() > 0) {
        auto data11_copy = _copyVector<11>(data11);
        bv.data11 = data11_copy;
      } 
      break;
    }
    case 12: {
      if (data12.size() > 0) {
        auto data12_copy = _copyVector<12>(data12);
        bv.data12 = data12_copy;
      } 
      break;
    }
    case 13: {
      if (data13.size() > 0) {
        auto data13_copy = _copyVector<13>(data13);
        bv.data13 = data13_copy;
      } 
      break;
    }
    case 14: {
      if (data14.size() > 0) {
        auto data14_copy = _copyVector<14>(data14);
        bv.data14 = data14_copy;
      } 
      break;
    }
    case 15: {
      if (data15.size() > 0) {
        auto data15_copy = _copyVector<15>(data15);
        bv.data15 = data15_copy;
      } 
      break;
    }
    case 16: {
      if (data16.size() > 0) {
        auto data16_copy = _copyVector<16>(data16);
        bv.data16 = data16_copy;
      } 
      break;
    }
    case 17: {
      if (data17.size() > 0) {
        auto data17_copy = _copyVector<17>(data17);
        bv.data17 = data17_copy;
      } 
      break;
    }
    case 18: {
      if (data18.size() > 0) {
        auto data18_copy = _copyVector<18>(data18);
        bv.data18 = data18_copy;
      } 
      break;
    }
    case 19: {
      if (data19.size() > 0) {
        auto data19_copy = _copyVector<19>(data19);
        bv.data19 = data19_copy;
      } 
      break;
    }
    case 20: {
      if (data20.size() > 0) {
        auto data20_copy = _copyVector<20>(data20);
        bv.data20 = data20_copy;
      } 
      break;
    }
    case 21: {
      if (data21.size() > 0) {
        auto data21_copy = _copyVector<21>(data21);
        bv.data21 = data21_copy;
      } 
      break;
    }
    case 22: {
      if (data22.size() > 0) {
        auto data22_copy = _copyVector<22>(data22);
        bv.data22 = data22_copy;
      } 
      break;
    }
    case 23: {
      if (data23.size() > 0) {
        auto data23_copy = _copyVector<23>(data23);
        bv.data23 = data23_copy;
      } 
      break;
    }
    case 24: {
      if (data24.size() > 0) {
        auto data24_copy = _copyVector<24>(data24);
        bv.data24 = data24_copy;
      } 
      break;
    }
    case 25: {
      if (data25.size() > 0) {
        auto data25_copy = _copyVector<25>(data25);
        bv.data25 = data25_copy;
      } 
      break;
    }
    case 26: {
      if (data26.size() > 0) {
        auto data26_copy = _copyVector<26>(data26);
        bv.data26 = data26_copy;
      } 
      break;
    }
    case 27: {
      if (data27.size() > 0) {
        auto data27_copy = _copyVector<27>(data27);
        bv.data27 = data27_copy;
      } 
      break;
    }
    case 28: {
      if (data28.size() > 0) {
        auto data28_copy = _copyVector<28>(data28);
        bv.data28 = data28_copy;
      } 
      break;
    }
    case 29: {
      if (data29.size() > 0) {
        auto data29_copy = _copyVector<29>(data29);
        bv.data29 = data29_copy;
      } 
      break;
    }
    case 30: {
      if (data30.size() > 0) {
        auto data30_copy = _copyVector<30>(data30);
        bv.data30 = data30_copy;
      } 
      break;
    }
    case 31: {
      if (data31.size() > 0) {
        auto data31_copy = _copyVector<31>(data31);
        bv.data31 = data31_copy;
      } 
      break;
    }
    case 32: {
      if (data32.size() > 0) {
        auto data32_copy = _copyVector<32>(data32);
        bv.data32 = data32_copy;
      } 
      break;
    }
    default: std::cerr << "unexpected b=" << b << std::endl; break;
  }

  return bv;
}

}  // namespace opossum
