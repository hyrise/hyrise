#include "buffer_managed_segment.hpp"
#include <boost/container/pmr/global_resource.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <span>

namespace hyrise {

boost::container::pmr::monotonic_buffer_resource create_buffer_resource_for_page(
    std::shared_ptr<BufferManager> buffer_manager, const PageID page_id) {
  const auto page = buffer_manager->get_page(page_id);
  auto test = std::span{page->data.data(), page->data.size()};
  return boost::container::pmr::monotonic_buffer_resource{page->data.data(), page->data.size(),
                                                          boost::container::pmr::null_memory_resource()};
}

template <typename T>
BufferManagedSegment<T>::BufferManagedSegment(std::shared_ptr<AbstractSegment> managed_segment,
                                              std::shared_ptr<BufferManager> buffer_manager)
    : AbstractSegment(data_type_from_type<T>()),
      _buffer_manager(buffer_manager),
      _page_id(_buffer_manager->new_page()),
      _page_resource(create_buffer_resource_for_page(buffer_manager, _page_id)) {
  // TODO: likley need to pin the page table/make sure that it is availably for alloction
  const auto allocator = PolymorphicAllocator<size_t>(&_page_resource);
  _managed_segment = managed_segment->copy_using_allocator(allocator);
}

template <typename T>
template <typename SegmentType>
void BufferManagedSegment<T>::load_managed_segment<SegmentType>() {
  if (_managed_segment) {
    return;
  }
  
  pmr_vector<int32_t> values(4, allocator);
  pmr_vector<bool> null_values(4, allocator);
  _managed_segment = std::make_shared<ValueSegment<int32_t>>(std::move(values), std::move(null_values));

  // This works
  // pmr_vector<int32_t> values(allocator);
  // auto prev_buffer = _page_resource.current_buffer();
  // values.reserve(4);
  // values.assign((int32_t*)prev_buffer, (int32_t*)_page_resource.current_buffer());
  // pmr_vector<bool> null_values(allocator);
  // prev_buffer = _page_resource.current_buffer();
  // null_values.reserve(4);
  // null_values.assign((bool*)prev_buffer,(bool*)_page_resource.current_buffer());
  // _managed_segment = std::make_shared<ValueSegment<int32_t>>(std::move(values), std::move(null_values));


//  pmr_vector<T> new_values(_values, alloc);  // NOLINT(cppcoreguidelines-slicing)
//   std::shared_ptr<AbstractSegment> copy;
//   if (is_nullable()) {
//     pmr_vector<bool> new_null_values(*_null_values, alloc);  // NOLINT(cppcoreguidelines-slicing) (see above)
//     copy = std::make_shared<ValueSegment<T>>(std::move(new_values), std::move(new_null_values));
//   } else {
//     copy = std::make_shared<ValueSegment<T>>(std::move(new_values));
//   }
//   copy->access_counter = access_counter;
//   return copy;
}

template <typename T>
void BufferManagedSegment<T>::unload_managed_segment() {
  _managed_segment = nullptr;
  // Release the page resource which resets the current buffer and next buffer size of the memory resource.
  // Deallocate does nothing in the monotonic_buffer_resource so that we can reuse the memory later again
  _page_resource.release();w
}

template <typename T>
void BufferManagedSegment<T>::unload_managed_segment() {
  _managed_segment = nullptr;
  // Release the page resource which resets the current buffer and next buffer size of the memory resource.
  // Deallocate does nothing in the monotonic_buffer_resource so that we can reuse the memory later again
  _page_resource.release();
}


// TODO: Create and unload segments via their constructor arguments and customized std::vectors

// template <typename T>
// BufferManagedSegment<T>::~BufferManagedSegment() {
//   _buffer_manager->remove_page(_page_id);
// }



template <typename T>
AllTypeVariant BufferManagedSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  return _managed_segment->operator[](chunk_offset);
}

template <typename T>
ChunkOffset BufferManagedSegment<T>::size() const {
  return _managed_segment->size();
}

template <typename T>
std::shared_ptr<AbstractSegment> BufferManagedSegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  Fail("Using copy_using_allocator is now allowed in BufferManagedSegment");
}

template <typename T>
size_t BufferManagedSegment<T>::memory_usage(const MemoryUsageCalculationMode mode) const {
  return _managed_segment->memory_usage(mode);  // TODO: This could be replaced with PAGE_SIZE
}

template <typename T>
std::shared_ptr<AbstractSegment> BufferManagedSegment<T>::pin_managed_segment() {
  _buffer_manager->pin_page(_page_id);
  load_managed_segment();
  return _managed_segment;
}

template <typename T>
void BufferManagedSegment<T>::unpin_managed_segment() {
  _buffer_manager->unpin_page(_page_id);
  // , [] {
  // If we are the only object that is left pointing at the managed segment, we should release forcefully
  // Assert(_managed_segment.use_count() == 1,
  //        "When the pin count is zero, there should be not other object expect for this BufferManagedSegment owning "
  //        "the managed segment.");
  // _managed_segment = nullptr;
  // TODO: Forcefully get rid of the value in shared after flushing e.g. with null_deleter
  // });
}

// TODO: Flushing shul
// TODO: Call swizzle and unswizzle if a segment in written or read. We can just write directly into the data.

EXPLICITLY_INSTANTIATE_DATA_TYPES(BufferManagedSegment);

}  // namespace hyrise