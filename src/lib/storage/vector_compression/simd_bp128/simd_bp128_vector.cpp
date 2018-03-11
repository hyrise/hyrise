#include "simd_bp128_vector.hpp"

namespace opossum {

SimdBp128Vector::SimdBp128Vector(pmr_vector<uint128_t> vector, size_t size) : _data{std::move(vector)}, _size{size} {}

const pmr_vector<uint128_t>& SimdBp128Vector::data() const { return _data; }

size_t SimdBp128Vector::_on_size() const { return _size; }
size_t SimdBp128Vector::_on_data_size() const { return sizeof(uint128_t) * _data.size(); }

std::unique_ptr<BaseVectorDecompressor> SimdBp128Vector::_on_create_base_decoder() const {
  return std::unique_ptr<BaseVectorDecompressor>{_on_create_decoder()};
}

std::unique_ptr<SimdBp128Decompressor> SimdBp128Vector::_on_create_decoder() const {
  return std::make_unique<SimdBp128Decompressor>(*this);
}

SimdBp128Iterator SimdBp128Vector::_on_begin() const { return SimdBp128Iterator{&_data, _size, 0u}; }

SimdBp128Iterator SimdBp128Vector::_on_end() const { return SimdBp128Iterator{nullptr, _size, _size}; }

std::unique_ptr<const BaseCompressedVector> SimdBp128Vector::_on_copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto data_copy = pmr_vector<uint128_t>{_data, alloc};
  return std::make_unique<SimdBp128Vector>(std::move(data_copy), _size);
}

}  // namespace opossum
