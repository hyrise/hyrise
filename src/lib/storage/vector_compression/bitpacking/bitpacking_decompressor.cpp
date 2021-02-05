#include "bitpacking_decompressor.hpp"

#include "bitpacking_vector.hpp"

namespace opossum {

BitpackingDecompressor::BitpackingDecompressor(const BitpackingVector& vector)
    : 
    _data{vector.data()}
      {
      
      }

}  // namespace opossum