#pragma once

#include <type_traits>

#include "ns_decoder.hpp"
#include "fixed_size_byte_aligned_vector.hpp"

#include "types.hpp"


namespace opossum {

/**
 * Implements the non-virtual interface of all decoders
 */
template <typename UnsignedIntType>
class FixedSizeByteAlignedDecoder : public NsDecoder<FixedSizeByteAlignedDecoder<UnsignedIntType>> {
 public:
  using Vector = FixedSizeByteAlignedVector<UnsignedIntType>;

 public:
  explicit FixedSizeByteAlignedDecoder(const Vector& vector) : _vector{vector} {}

  uint32_t _on_get(size_t i) const {
    return _vector.data()[i];
  }

  size_t _on_size() const {
    return _vector.size();
  }

  auto _on_cbegin() const {
    return ConstIterator{_vector.data().cbegin()};
  }

  auto _on_cend() const {
    return ConstIterator{_vector.data().cend()};
  }

 private:
  const Vector& _vector;

 public:
  class ConstIterator : public BaseNsIterator<ConstIterator> {
   public:
    using ValueIterator = typename pmr_vector<UnsignedIntType>::const_iterator;

   public:
    ConstIterator(const ValueIterator& value_it) : _value_it{value_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_value_it; }

    bool equal(const ConstIterator& other) const { return _value_it == other._value_it; }

    uint32_t dereference() const { return *_value_it; }

   private:
    ValueIterator _value_it;
  };
};

}  // namespace opossum
