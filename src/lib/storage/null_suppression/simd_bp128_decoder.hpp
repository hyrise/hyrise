#pragma once

#include <emmintrin.h>

#include <array>

#include "ns_decoder.hpp"
#include "simd_bp128_vector.hpp"
#include "simd_bp128_packing.hpp"

#include "types.hpp"


namespace opossum {

class SimdBp128Decoder : public NsDecoder<SimdBp128Decoder> {
 public:
  using Vector = SimdBp128Vector;

 public:
  SimdBp128Decoder(const Vector& vector) : _vector{vector} {}

  class ConstIterator;

  auto _on_cbegin() {
    return ConstIterator{&_vector.data(), _vector.size()};
  }

  auto _on_cend() {
    return ConstIterator{nullptr, _vector.size()};
  }

  uint32_t _on_get(size_t i) {
    return 0u;
  }

  size_t _on_size() {
    return _vector.size();
  }

 private:
  const Vector& _vector;

 public:
  class ConstIterator : public BaseNsIterator<ConstIterator> {
   private:
    using Packing = SimdBp128Packing;

   public:
    ConstIterator(const pmr_vector<__m128i>* data, size_t absolute_index = 0u)
        : _data{data},
          _data_index{0u},
          _absolute_index{absolute_index},
          _current_meta_info_index{0u},
          _current_block{std::make_unique<std::array<uint32_t, Packing::block_size>>()},
          _current_block_index{0u} {
      read_meta_info();
      unpack_block();
    }

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_absolute_index;
      ++_current_block_index;

      if (_current_block_index >= Packing::block_size) {
        ++_current_meta_info_index;

        if (_current_meta_info_index >= Packing::blocks_in_meta_block) {
          read_meta_info();
          unpack_block();
        } else {
          unpack_block();
        }
      }
    }

    bool equal(const ConstIterator& other) const { return _absolute_index == other._absolute_index; }

    uint32_t dereference() const {
      return (*_current_block)[_current_block_index];
    }

   private:
    void read_meta_info();
    void unpack_block();

   private:
    const pmr_vector<__m128i>* _data;

    size_t _data_index;
    size_t _absolute_index;

    std::array<uint8_t, Packing::blocks_in_meta_block> _current_meta_info;
    size_t _current_meta_info_index;

    const std::unique_ptr<std::array<uint32_t, Packing::block_size>> _current_block;
    size_t _current_block_index;
  };
};

}  // namespace opossum
