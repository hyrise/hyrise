#include "simd_bp128_vector.hpp"

#include "simd_bp128_decoder.hpp"

namespace opossum {

SimdBp128Vector::SimdBp128Vector(pmr_vector<uint128_t> vector, size_t size) : _data{std::move(vector)}, _size{size} {}

const pmr_vector<uint128_t>& SimdBp128Vector::data() const { return _data; }

size_t SimdBp128Vector::_on_size() const { return _size; }
size_t SimdBp128Vector::_on_data_size() const { return sizeof(uint128_t) * _data.capacity(); }

std::unique_ptr<BaseZeroSuppressionDecoder> SimdBp128Vector::_on_create_base_decoder() const {
  return std::unique_ptr<BaseZeroSuppressionDecoder>{_on_create_decoder()};
}

std::unique_ptr<SimdBp128Decoder> SimdBp128Vector::_on_create_decoder() const {
  return std::make_unique<SimdBp128Decoder>(*this);
}

auto SimdBp128Vector::_on_cbegin() const -> ConstIterator { return ConstIterator{&_data, _size, 0u}; }

auto SimdBp128Vector::_on_cend() const -> ConstIterator { return ConstIterator{nullptr, _size, _size}; }

std::shared_ptr<BaseZeroSuppressionVector> SimdBp128Vector::_on_copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto data_copy = pmr_vector<uint128_t>{_data, alloc};
  return std::allocate_shared<SimdBp128Vector>(alloc, std::move(data_copy), _size);
}

}  // namespace opossum
