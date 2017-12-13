#pragma once

#include <boost/hana/fold.hpp>
#include <boost/hana/value.hpp>

#include <cstdint>
#include <memory>

#include "base_zero_suppression_vector.hpp"
#include "zs_type.hpp"

#include "types.hpp"

namespace opossum {

std::unique_ptr<BaseZeroSuppressionVector> encode_by_zs_type(ZsType type, const pmr_vector<uint32_t>& vector,
                                                             const PolymorphicAllocator<size_t>& alloc);

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
