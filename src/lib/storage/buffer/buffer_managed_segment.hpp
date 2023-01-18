#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include "storage/abstract_segment.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/frame.hpp"
#include "storage/buffer/page.hpp"
#include "storage/value_segment.hpp"

namespace hyrise {

template <typename T>
class BufferManagedSegment : public AbstractSegment {
 public:
  explicit BufferManagedSegment(std::shared_ptr<AbstractSegment> managed_segment,
                                std::shared_ptr<BufferManager> buffer_manager);
  // ~BufferManagedSegment() override;

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;
  ChunkOffset size() const final;
  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;
  size_t memory_usage(const MemoryUsageCalculationMode mode) const final;

  std::shared_ptr<AbstractSegment> pin_managed_segment();
  void unpin_managed_segment();

  void load_managed_segment();
  void unload_managed_segment();

 private:
  std::shared_ptr<BufferManager> _buffer_manager;
  PageID _page_id;  // TODO: Try to make this const
  boost::container::pmr::monotonic_buffer_resource _page_resource;
  std::shared_ptr<AbstractSegment> _managed_segment;  // TODO: If use usecount is == 1, we can probably unpin it
};

EXPLICITLY_DECLARE_DATA_TYPES(BufferManagedSegment);

}  // namespace hyrise