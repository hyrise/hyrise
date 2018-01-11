#pragma once

#include <boost/hana/fold.hpp>
#include <boost/hana/value.hpp>

#include "vectors.hpp"
#include "zs_vector_type.hpp"

namespace opossum {

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
