#include "simd_bp128_vector.hpp"


namespace opossum {

SimdBp128Vector::SimdBp128Vector(pmr_vector<__m128i> vector, size_t size)
    : _data{std::move(vector)}, _size{size} {}

size_t SimdBp128Vector::size() const { return _size; }

size_t SimdBp128Vector::data_size() const { return sizeof(__m128i) * _data.size(); }

NsType SimdBp128Vector::type() const { return NsType::SimdBp128; }

const pmr_vector<__m128i>& SimdBp128Vector::data() const { return _data; }

}  // namespace opossum
