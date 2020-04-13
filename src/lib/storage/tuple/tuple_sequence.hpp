#pragma once

#include "storage/chunk.hpp"

namespace opossum
{

class TupleFormat;

template<typename TupleSizePolicy>
class TupleSequence : public TupleSizePolicy
{
public:
  static TupleSequence interleave(const TupleFormat& format, const Segments& segments);

private:

};

}

#incldue "storage/tuple/tuple_sequence.inl"
