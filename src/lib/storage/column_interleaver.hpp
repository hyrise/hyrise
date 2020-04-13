#pragma once

#include "storage/chunk.hpp"
#include "storage/tuple/tuple_sequence.hpp"

namespace opossum {

class ColumnInterleaver {
 public:
  TupleSequence interleave(const Segments& segments) const;
};

}  // namespace opossum
