#pragma once

#include <boost/hana/fold.hpp>
#include <boost/hana/value.hpp>

#include <cstdint>
#include <memory>

#include "base_ns_vector.hpp"
#include "ns_type.hpp"

#include "types.hpp"

namespace opossum {

std::unique_ptr<BaseNsVector> encode_by_ns_type(NsType type, const pmr_vector<uint32_t>& vector,
                                                const PolymorphicAllocator<size_t>& alloc);

template <typename Functor>
void resolve_ns_vector_type(const BaseNsVector& vector, const Functor& functor) {
  hana::fold(ns_vector_for_type, false, [&](auto match_found, auto pair) {
    const auto vector_type_c = hana::first(pair);
    const auto vector_t = hana::second(pair);

    if (!match_found && (vector_type_c() == vector.type())) {
      using NsVectorType = typename decltype(vector_t)::type;
      functor(static_cast<const NsVectorType&>(vector));

      return true;
    }

    return match_found;
  });
}

}  // namespace opossum
