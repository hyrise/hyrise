#pragma once

#include <boost/hana/fold.hpp>
#include <boost/hana/value.hpp>

// Include your compressed vector file here!
#include "bitpacking/bitpacking_vector.hpp"
#include "fixed_width_integer/fixed_width_integer_vector.hpp"

#include "compressed_vector_type.hpp"

namespace hyrise {

/**
 * @brief Resolves the type of a compressed vector
 *
 * @param func is a generic lambda or similar accepting a const reference to
 *        specific implementation of a compressed vector (such as BitPackingVector).
 *
 *
 * Example
 *
 *   resolve_compressed_vector_type(base_vector, [&](const auto& vector) {
 *     for (auto it = vector.cbegin(); it != vector.cend(); ++it) { ... }
 *   });
 */
template <typename Functor>
void resolve_compressed_vector_type(const BaseCompressedVector& vector, const Functor& functor) {
  hana::fold(compressed_vector_for_type, false, [&](auto match_found, auto pair) {
    const auto vector_type_c = hana::first(pair);
    const auto vector_t = hana::second(pair);

    if (!match_found && (hana::value(vector_type_c) == vector.type())) {
      using CompressedVectorType = typename decltype(vector_t)::type;
      functor(static_cast<const CompressedVectorType&>(vector));

      return true;
    }

    return match_found;
  });
}

}  // namespace hyrise
