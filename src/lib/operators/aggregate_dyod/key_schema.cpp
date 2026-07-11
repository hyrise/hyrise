#include "operators/aggregate_dyod/key_schema.hpp"

// Method definitions for AggregateDYOD's key_schema are added in a later step. This translation unit currently exists to
// compile the header standalone (verifying it is self-contained) and to host the forthcoming out-of-line and
// explicitly-instantiated template definitions.

namespace hyrise {
void StringSpillBuffer::clear() {
  _size = 0;
}
}  // namespace hyrise
