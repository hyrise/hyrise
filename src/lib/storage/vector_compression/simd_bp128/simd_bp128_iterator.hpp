#pragma once

#include <array>
#include <memory>

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "oversized_types.hpp"
#include "simd_bp128_packing.hpp"

#include "types.hpp"

namespace opossum {

class SimdBp128Iterator : public BaseCompressedVectorIterator<SimdBp128Iterator> {
 public:
  using Packing = SimdBp128Packing;

 public:
  SimdBp128Iterator(const pmr_vector<uint128_t>* data, size_t size, size_t absolute_index = 0u);
  SimdBp128Iterator(const SimdBp128Iterator& other);

  SimdBp128Iterator(SimdBp128Iterator&& other) = default;
  ~SimdBp128Iterator() = default;

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() {
    ++_absolute_index;
    ++_current_meta_block_index;

    if (_current_meta_block_index >= Packing::meta_block_size && _absolute_index < _size) {
      _unpack_next_meta_block();
    }
  }

  bool equal(const SimdBp128Iterator& other) const { return _absolute_index == other._absolute_index; }

  uint32_t dereference() const { return (*_current_meta_block)[_current_meta_block_index]; }

 private:
  void _unpack_next_meta_block();

  void _read_meta_info();
  void _unpack_block(uint8_t meta_info_index);

 private:
  const pmr_vector<uint128_t>* _data;
  const size_t _size;

  size_t _data_index;
  size_t _absolute_index;

  std::array<uint8_t, Packing::blocks_in_meta_block> _current_meta_info{};

  const std::unique_ptr<std::array<uint32_t, Packing::meta_block_size>> _current_meta_block;
  size_t _current_meta_block_index;
};

}  // namespace opossum
