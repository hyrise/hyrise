#include "simd_bp128_vector.hpp"

namespace opossum {

SimdBp128Vector::SimdBp128Vector(pmr_vector<__m128i> vector, size_t size) : _data{std::move(vector)}, _size{size} {}

size_t SimdBp128Vector::_on_size() const { return _size; }
size_t SimdBp128Vector::_on_data_size() const { return sizeof(__m128i) * _data.size(); }

std::unique_ptr<SimdBp128Decoder> SimdBp128Vector::_on_create_decoder() const {
  return std::make_unique<SimdBp128Decoder>(*this);
}

auto _on_cbegin() const { return ConstIterator{_data, _size, 0u}; }
auto _on_cend() const { return ConstIterator{nullptr, _size, _size}; }

}  // namespace opossum
