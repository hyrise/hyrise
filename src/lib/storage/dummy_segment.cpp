#include <stdexcept>
#include <cstddef>
#include <memory>

#include "all_type_variant.hpp"
#include "dummy_segment.hpp"
#include "storage/abstract_segment.hpp"
#include "types.hpp"

namespace hyrise {
DummySegment::DummySegment(DataType data_type, ChunkOffset alleged_size) : AbstractSegment(data_type), _own_data_type(data_type), _alleged_size(alleged_size) {}

bool DummySegment::supports_reencoding() const {
  return false;
}

AllTypeVariant DummySegment::operator[](const ChunkOffset /* chunk_offset */) const {
    throw std::runtime_error("A dummy segment does not hold any data and can there not be accessed by operator[].");
}
ChunkOffset DummySegment::size() const {
    return _alleged_size;
}
std::shared_ptr<AbstractSegment> DummySegment::copy_using_allocator(const PolymorphicAllocator<size_t>& /* alloc */) const {
    return std::make_shared<DummySegment>(_own_data_type, _alleged_size);
}
size_t DummySegment::memory_usage(const MemoryUsageCalculationMode mode) const {
    return sizeof(*this);
}
} // namespace hyrise
