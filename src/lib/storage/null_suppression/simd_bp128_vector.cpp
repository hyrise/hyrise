#include "simd_bp128_vector.hpp"


namespace opossum {

SimdBp128Vector::SimdBp128Vector(pmr_vector<__m128i> vector, size_t size) : _vector{std::move(vector)}, _size{size} {}

uint32_t SimdBp128Vector::get(const size_t i) const {
  Fail("Not yet implemented.");
  return {};
}

size_t SimdBp128Vector::size() const {
  return _size;
}

const pmr_vector<__m128i>& SimdBp128Vector::data() const {
  return _data;
}

}  // namespace opossum