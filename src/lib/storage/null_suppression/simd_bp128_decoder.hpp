#pragma once

#include <emmintrin.h>

#include "ns_decoder.hpp"
#include "fixed_size_byte_aligned_vector.hpp"

#include "types.hpp"


namespace opossum {

/**
 * Implements the non-virtual interface of all decoders
 */
template <typename UnsignedIntType>
class SimdBp128Decoder : public NsDecoder<SimdBp128Decoder<UnsignedIntType>> {
 public:
  using Vector = SimdBp128Vector;

 public:
  SimdBp128Decoder(const Vector& vector) : _vector{vector} {}

  class ConstIterator;

  auto _on_cbegin() {
    return ConstIterator{&_vector.data()};
  }

  auto _on_cend() {
    return ConstIterator{nullptr, _vector.size()};
  }

  uint32_t _on_get(size_t i) {
    return _vector.data()[i];
  }

  size_t _on_size() {
    return _vector.size();
  }

 private:
  const Vector& _vector;

 public:
  class ConstIterator : public BaseNsIterator<ConstIterator> {
   public:
    ConstIterator(const pmr_vector<__m128>* data, size_t absolute_index = 0u)
        : _data{data},
          _data_index{0u},
          _absolute_index{absolute_index},
          _current_meta_info_index{0u},
          _current_block{std::make_unique<std::array<uint32_t, Vector::block_size>>()},
          _current_block_index{0u} {
      read_meta_info();
      unpack_block();
    }

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_absolute_index;
      ++_current_block_index;

      if (_current_block_index >= Vector::block_size) {
        ++_current_meta_info_index;

        if (_current_meta_info_index >= Vector::blocks_in_meta_block) {
          read_meta_info();
          unpack_block();
        } else {
          unpack_block();
        }
      }
    }

    bool equal(const ConstIterator& other) const { return _current_absolute_index == other._current_absolute_index; }

    uint32_t dereference() const {
      return _current_block[_current_block_index];
    }

   private:
    void read_meta_info();
    void unpack_block();

   private:
    const pmr_vector<__m128>* _data;

    size_t _absolute_index;
    size_t _data_index;

    std::array<uint32_t, Vector::blocks_in_meta_block> _current_meta_info;
    size_t _current_meta_info_index;

    const std::unique_ptr<std::array<uint32_t, Vector::block_size>> _current_block;
    size_t _current_block_index;
  }
};

}  // namespace opossum
