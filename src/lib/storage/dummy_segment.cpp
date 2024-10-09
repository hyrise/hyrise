#include "dummy_segment.hpp"

namespace hyrise {
DummySegment::DummySegment(DataType data_type, ChunkOffset allged_size) : AbstractSegment(data_type), _own_data_type(data_type), _alleged_size(allged_size) {}

AllTypeVariant DummySegment::operator[](const ChunkOffset chunk_offset) const {
    throw std::runtime_error("A dummy segment does not hold any data and can there not be accessed by operator[].");
}
ChunkOffset DummySegment::size() const {
    return _alleged_size;
}
std::shared_ptr<AbstractSegment> DummySegment::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
    return std::make_shared<DummySegment>(_own_data_type, _alleged_size);
}
size_t DummySegment::memory_usage(const MemoryUsageCalculationMode mode) const {
    return sizeof(*this);
}
} // namespace hyrise
