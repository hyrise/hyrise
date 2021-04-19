#pragma once

#include "bitpacking_vector_type.hpp"
#include "compact_vector.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"

namespace opossum {

class BitPackingVector;

class BitPackingDecompressor : public BaseVectorDecompressor {
 public:
  explicit BitPackingDecompressor(const pmr_compact_vector<uint32_t>& data) : _data{data} {}
  BitPackingDecompressor(const BitPackingDecompressor& other) = default;
  BitPackingDecompressor(BitPackingDecompressor&& other) = default;

  BitPackingDecompressor& operator=(const BitPackingDecompressor& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign BitPackingDecompressor");
    return *this;
  }
  BitPackingDecompressor& operator=(BitPackingDecompressor&& other) {
    DebugAssert(&_data == &other._data, "Cannot reassign BitPackingDecompressor");
    return *this;
  }

  ~BitPackingDecompressor() override = default;

  uint32_t get(size_t i) final { return _data[i]; }

  size_t size() const final { return _data.size(); }

 private:
  const pmr_compact_vector<uint32_t>& _data;
};

}  // namespace opossum
