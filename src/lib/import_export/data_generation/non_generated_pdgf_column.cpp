#include "all_type_variant.hpp"
#include "non_generated_pdgf_column.hpp"
#include "storage/dummy_segment.hpp"

namespace hyrise {
BaseNonGeneratedPDGFColumn::BaseNonGeneratedPDGFColumn() {}

template <typename T>
NonGeneratedPDGFColumn<T>::NonGeneratedPDGFColumn(std::string name, DataType type) : _name(name), _type(type) {}

template <typename T>
std::string NonGeneratedPDGFColumn<T>::name() {
  return _name;
}

template <typename T>
DataType NonGeneratedPDGFColumn<T>::type() {
  return _type;
}

template <typename T>
std::shared_ptr<AbstractSegment> NonGeneratedPDGFColumn<T>::build_segment(ChunkOffset chunk_size) {
  return std::make_shared<DummySegment<T>>(chunk_size);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(NonGeneratedPDGFColumn);
} // namespace hyrise
