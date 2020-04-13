#pragma once

#include "storage/chunk.hpp"
#include "storage/tuple/tuple_format.hpp"
#include "storage/tuple/tuple_sequence.hpp"

namespace opossum {

template<typename TupleSizePolicy>
static TupleSequence<TupleSizePolicy> TupleSequence<TupleSizePolicy>::interleave(const Segments& segments) {

}

}  // namespace opossum
