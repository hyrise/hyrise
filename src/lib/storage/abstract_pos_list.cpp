#include "abstract_pos_list.hpp"
#include "pos_list.hpp"

namespace opossum {
  template <>
  AbstractPosList::PosListIterator<true>::DereferenceReturnType AbstractPosList::PosListIterator<true>::dereference() const {
    DebugAssert(_chunk_offset < _max_size, "You foool");

    // DANGER!
    // If you have runtime errors (segfaults, terminate, ...) - this might be your spot
    return static_cast<PosList&>(*_pl_pointer)[_chunk_offset];
  }

  template <>
  AbstractPosList::PosListIterator<false>::DereferenceReturnType AbstractPosList::PosListIterator<false>::dereference() const {
    // As of 986b98ce, we still come by here very often, even if we're only creating PosList instances in the code
    // -> Somewhere, the type is not resolved.
    /* std::cout << "Slow dereference\n"; */

    DebugAssert(_chunk_offset < _max_size, "You foool");

#if 1
    return (*_pl_pointer)[_chunk_offset];
#else
    return static_cast<const PosList&>(*_pl_pointer)[_chunk_offset];
#endif
  }
}  // namespace opossum
