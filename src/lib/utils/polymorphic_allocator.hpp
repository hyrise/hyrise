#pragma once

#include <boost/container/pmr/polymorphic_allocator.hpp> // NEEDEDINCLUDE

namespace opossum {

/** We use vectors with custom allocators, e.g, to bind the data object to
 * specific NUMA nodes. This is mainly used in the data objects, i.e.,
 * Chunk, ValueSegment, DictionarySegment, ReferenceSegment and attribute vectors.
 * The PolymorphicAllocator provides an abstraction over several allocation
 * methods by adapting to subclasses of boost::container::pmr::memory_resource.
 */

template <typename T>
using PolymorphicAllocator = boost::container::pmr::polymorphic_allocator<T>;

}
