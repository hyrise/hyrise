#pragma once

#include <cstdint>
#include <memory>

#include "ns_type.hpp"
#include "base_ns_vector.hpp"

#include "types.hpp"

namespace opossum {

std::unique_ptr<BaseNsVector> encode_by_ns_type(NsType type, const pmr_vector<uint32_t>& vector,
                                                const PolymorphicAllocator<size_t>& alloc);

template <typename Functor>
void resolve_ns_vector_type(const BaseNsVector& vector, const Functor& func) {
  hana::fold(ns_vector_for_type, false, [&](auto match_found, auto ns_pair) {
    if (!match_found && (hana::first(ns_pair) == vector.type())) {
      using NsVectorType = typename decltype(+hana::second(ns_pair))::type;
      func(static_cast<const NsVectorType&>(vector));

      return true;
    }

    return false;
  });
}

}  // namespace opossum
