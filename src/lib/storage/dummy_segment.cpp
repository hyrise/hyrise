#include <stdexcept>
#include <cstddef>
#include <memory>

#include "all_type_variant.hpp"
#include "dummy_segment.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "types.hpp"

namespace hyrise {
template <typename T>
DummySegment<T>::DummySegment(ChunkOffset alleged_size)
    : AbstractSegment(data_type_from_type<T>()), empty_value_segment(ValueSegment<T>{false, ChunkOffset{0}}), _alleged_size(alleged_size) {}

template <typename T>
bool DummySegment<T>::supports_reencoding() const {
  return false;
}

template <typename T>
bool DummySegment<T>::has_actual_data() const {
  return false;
}

template <typename T>
AllTypeVariant DummySegment<T>::operator[](const ChunkOffset /* chunk_offset */) const {
    throw std::runtime_error("A dummy segment does not hold any data and can there not be accessed by operator[].");
}

template <typename T>
std::optional<T> DummySegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  throw std::runtime_error("A dummy segment does not hold any data and can there not be accessed by by chunk_offset.");
}

template <typename T>
ChunkOffset DummySegment<T>::size() const {
    return _alleged_size;
}

template <typename T>
std::shared_ptr<AbstractSegment> DummySegment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& /* alloc */) const {
    return std::make_shared<DummySegment>(_alleged_size);
}

template <typename T>
size_t DummySegment<T>::memory_usage(const MemoryUsageCalculationMode mode) const {
    return sizeof(*this);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DummySegment);
} // namespace hyrise
