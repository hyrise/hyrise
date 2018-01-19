#pragma once

#include <boost/hana/fold.hpp>
#include <boost/hana/value.hpp>

// Include your zero suppression vector file here!
#include "fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"
#include "simd_bp128/simd_bp128_vector.hpp"

#include "zs_vector_type.hpp"

namespace opossum {

/**
 * @brief Resolves the type of a zero suppression vector
 *
 * @param func is a generic lambda or similar accepting a const reference to
 *        specific implementation of a zero suppression vector (such as SimdBp128Vector).
 *
 *
 * Example
 *
 *   resolve_zs_vector_type(base_vector, [&](const auto& vector) {
 *     for (auto it = vector.cbegin(); it != vector.cend(); ++it) { ... }
 *   });
 */
template <typename Functor>
void resolve_zs_vector_type(const BaseZeroSuppressionVector& vector, const Functor& functor) {
  hana::fold(zs_vector_for_type, false, [&](auto match_found, auto pair) {
    const auto vector_type_c = hana::first(pair);
    const auto vector_t = hana::second(pair);

    if (!match_found && (hana::value(vector_type_c) == vector.type())) {
      using ZsVectorType = typename decltype(vector_t)::type;
      functor(static_cast<const ZsVectorType&>(vector));

      return true;
    }

    return match_found;
  });
}

}  // namespace opossum
