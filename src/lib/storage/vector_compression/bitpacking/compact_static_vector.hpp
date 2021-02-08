#pragma once

#include "vector_types.hpp"        

namespace opossum {

  class CompactStaticVector {
     public:

      CompactStaticVector(size_t b, const PolymorphicAllocator<size_t>& alloc) : _b{b} {
          switch (b) {
            case 1: data1 = pmr_bitpacking_vector<uint32_t,1>(alloc); break;
            case 2: data2 = pmr_bitpacking_vector<uint32_t,2>(alloc); break;
            case 3: data3 = pmr_bitpacking_vector<uint32_t,3>(alloc); break;
            case 4: data4 = pmr_bitpacking_vector<uint32_t,4>(alloc); break;
            case 5: data5 = pmr_bitpacking_vector<uint32_t,5>(alloc); break;
            case 6: data6 = pmr_bitpacking_vector<uint32_t,6>(alloc); break;
            case 7: data7 = pmr_bitpacking_vector<uint32_t,7>(alloc); break;
            case 8: data8 = pmr_bitpacking_vector<uint32_t,8>(alloc); break;
            case 9: data9 = pmr_bitpacking_vector<uint32_t,9>(alloc); break;
            case 10: data10 = pmr_bitpacking_vector<uint32_t,10>(alloc); break;
            case 11: data11 = pmr_bitpacking_vector<uint32_t,11>(alloc); break;
            case 12: data12 = pmr_bitpacking_vector<uint32_t,12>(alloc); break;
            case 13: data13 = pmr_bitpacking_vector<uint32_t,13>(alloc); break;
            case 14: data14 = pmr_bitpacking_vector<uint32_t,14>(alloc); break;
            case 15: data15 = pmr_bitpacking_vector<uint32_t,15>(alloc); break;
            case 16: data16 = pmr_bitpacking_vector<uint32_t,16>(alloc); break;
            case 17: data17 = pmr_bitpacking_vector<uint32_t,17>(alloc); break;
            case 18: data18 = pmr_bitpacking_vector<uint32_t,18>(alloc); break;
            case 19: data19 = pmr_bitpacking_vector<uint32_t,19>(alloc); break;
            case 20: data20 = pmr_bitpacking_vector<uint32_t,20>(alloc); break;
            case 21: data21 = pmr_bitpacking_vector<uint32_t,21>(alloc); break;
            case 22: data22 = pmr_bitpacking_vector<uint32_t,22>(alloc); break;
            case 23: data23 = pmr_bitpacking_vector<uint32_t,23>(alloc); break;
            case 24: data24 = pmr_bitpacking_vector<uint32_t,24>(alloc); break;
            case 25: data25 = pmr_bitpacking_vector<uint32_t,25>(alloc); break;
            case 26: data26 = pmr_bitpacking_vector<uint32_t,26>(alloc); break;
            case 27: data27 = pmr_bitpacking_vector<uint32_t,27>(alloc); break;
            case 28: data28 = pmr_bitpacking_vector<uint32_t,28>(alloc); break;
            case 29: data29 = pmr_bitpacking_vector<uint32_t,29>(alloc); break;
            case 30: data30 = pmr_bitpacking_vector<uint32_t,30>(alloc); break;
            case 31: data31 = pmr_bitpacking_vector<uint32_t,31>(alloc); break;
            case 32: data32 = pmr_bitpacking_vector<uint32_t,32>(alloc); break;
            default: std::cerr << "unexpected b=" << _b << std::endl; break;
          }
      }

      size_t b() const {
        return _b;
      }

      void push_back(uint32_t value) {
          switch (_b) {
            case 1: data1.push_back(value); break;
            case 2: data2.push_back(value); break;
            case 3: data3.push_back(value); break;
            case 4: data4.push_back(value); break;
            case 5: data5.push_back(value); break;
            case 6: data6.push_back(value); break;
            case 7: data7.push_back(value); break;
            case 8: data8.push_back(value); break;
            case 9: data9.push_back(value); break;
            case 10: data10.push_back(value); break;
            case 11: data11.push_back(value); break;
            case 12: data12.push_back(value); break;
            case 13: data13.push_back(value); break;
            case 14: data14.push_back(value); break;
            case 15: data15.push_back(value); break;
            case 16: data16.push_back(value); break;
            case 17: data17.push_back(value); break;
            case 18: data18.push_back(value); break;
            case 19: data19.push_back(value); break;
            case 20: data20.push_back(value); break;
            case 21: data21.push_back(value); break;
            case 22: data22.push_back(value); break;
            case 23: data23.push_back(value); break;
            case 24: data24.push_back(value); break;
            case 25: data25.push_back(value); break;
            case 26: data26.push_back(value); break;
            case 27: data27.push_back(value); break;
            case 28: data28.push_back(value); break;
            case 29: data29.push_back(value); break;
            case 30: data30.push_back(value); break;
            case 31: data31.push_back(value); break;
            case 32: data32.push_back(value); break;
            default: std::cerr << "unexpected b=" << _b << std::endl; break;
        }
      }

      size_t size() const {
          switch (_b) {
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
            default: std::cerr << "unexpected b=" << _b << std::endl; return 0;
        }
      }

      size_t bytes() const {
          switch (_b) {
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
            default: std::cerr << "unexpected b=" << _b << std::endl; return 0;
        }
      }

      size_t get(int i) const {
          switch (_b) {
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
            default: std::cerr << "unexpected b=" << _b << std::endl; return 0;
        }
      }

    private:
      size_t _b;

      pmr_bitpacking_vector<uint32_t, 1> data1;
      pmr_bitpacking_vector<uint32_t, 2> data2;
      pmr_bitpacking_vector<uint32_t, 3> data3;
      pmr_bitpacking_vector<uint32_t, 4> data4;
      pmr_bitpacking_vector<uint32_t, 5> data5;
      pmr_bitpacking_vector<uint32_t, 6> data6;
      pmr_bitpacking_vector<uint32_t, 7> data7;
      pmr_bitpacking_vector<uint32_t, 8> data8;
      pmr_bitpacking_vector<uint32_t, 9> data9;
      pmr_bitpacking_vector<uint32_t, 10> data10;
      pmr_bitpacking_vector<uint32_t, 11> data11;
      pmr_bitpacking_vector<uint32_t, 12> data12;
      pmr_bitpacking_vector<uint32_t, 13> data13;
      pmr_bitpacking_vector<uint32_t, 14> data14;
      pmr_bitpacking_vector<uint32_t, 15> data15;
      pmr_bitpacking_vector<uint32_t, 16> data16;
      pmr_bitpacking_vector<uint32_t, 17> data17;
      pmr_bitpacking_vector<uint32_t, 18> data18;
      pmr_bitpacking_vector<uint32_t, 19> data19;
      pmr_bitpacking_vector<uint32_t, 20> data20;
      pmr_bitpacking_vector<uint32_t, 21> data21;
      pmr_bitpacking_vector<uint32_t, 22> data22;
      pmr_bitpacking_vector<uint32_t, 23> data23;
      pmr_bitpacking_vector<uint32_t, 24> data24;
      pmr_bitpacking_vector<uint32_t, 25> data25;
      pmr_bitpacking_vector<uint32_t, 26> data26;
      pmr_bitpacking_vector<uint32_t, 27> data27;
      pmr_bitpacking_vector<uint32_t, 28> data28;
      pmr_bitpacking_vector<uint32_t, 29> data29;
      pmr_bitpacking_vector<uint32_t, 30> data30;
      pmr_bitpacking_vector<uint32_t, 31> data31;
      pmr_bitpacking_vector<uint32_t, 32> data32;
  };
} // namespace Opposum