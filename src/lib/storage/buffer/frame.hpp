#pragma once

#include <atomic>
#include <mutex>
#include "storage/buffer/types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class Frame {
 public:
  enum class State { Evicted, Resident };

  const PageSizeType size_type;
  const PageType page_type;
  const PageID page_id;

  explicit Frame(const PageID page_id, const PageSizeType size_type, const PageType page_type,
                 std::byte* data = nullptr)
      : page_id(page_id), size_type(size_type), page_type(page_type), data(data) {}

  ~Frame();
  // State variables
  std::atomic_uint32_t pin_count{0};
  std::atomic<State> state;
  std::atomic_bool dirty{false};

  // Reference bit that is used for second chance eviction
  std::atomic_bool referenced{false};

  // Used for eviction_queue
  std::atomic_uint64_t eviction_timestamp{0};

  // Pointer to raw data in volatile region
  std::byte* data;

  // Mutex used for safe concurrent access
  std::mutex latch;

  // Pointer to the sibling frame. This can be a DRAM frame or a NUMA frame.
  FramePtr sibling_frame;

  // Check if the frame can be evicted based on its state
  bool can_evict() const;

  // Set the frame to evicted state
  void set_evicted();

  // Try setting the dirty flag. It won't be reset if it is already set.
  void try_set_dirty(const bool new_dirty);

  // Check if the frame is resident
  bool is_resident() const;

  // Set the frame to resident state
  void set_resident();

  // Check if the frame is pinned
  bool is_pinned() const;

  // Check if the frame is invalid (e.g. for a dummy frame)
  bool is_invalid() const;

  // Check if the frame is dirty
  bool is_dirty() const;

  // Clear the frame
  void clear();

  // Pin the frame
  void pin();

  // Unpin the frame and return true if the frame is now unpinned. Otherwise return false.
  bool unpin();

  // Wait until the frame is unpinned. This yield the current threads and throws an exception after a fixed timout
  void wait_until_unpinned() const;

  // Check if the frame is evictable and if it is, set the reference bit to false and return true. Otherwise return
  bool try_second_chance_evictable();

  // Set the reference bit to true
  void set_referenced();

  // Check if the reference bit is set
  bool is_referenced() const;

  // Manually increase the reference count. This function is only cautionary used on the BufferPoolAllocator
  void increase_ref_count();

  // Manually decrease the reference count. This function is only cautionary used on the BufferPoolAllocator
  void decrease_ref_count();

  // Returns the internal reference count. This function should only be used for testing and debugging purposes.
  std::size_t _internal_ref_count() const;

  // Clone the frame and attach it as sibling. A NUMA frame gets a DRAM frame and visa-versa TODO: atomically?
  template <PageType clone_page_type>
  FramePtr clone_and_attach_sibling();

  // Copy the data from the source frame to the target frame using memcpy
  void copy_data_to(const FramePtr& target_frame) const;

 private:
  // Friend function used by the FramePtr intrusive_ptr to increase the ref_count. This functions should not be called directly.
  friend void intrusive_ptr_add_ref(Frame* frame);

  // Friend function used by the FramePtr intrusive_ptr to decrease the ref_count. This functions also avoids circular dependencies between the sibling frame.This functions should not be called directly.
  friend void intrusive_ptr_release(Frame* frame);

  // Current reference count of the frame
  std::atomic_uint32_t _ref_count;
};

template <typename... Args>
FramePtr make_frame(Args&&... args) {
  return FramePtr(new Frame(std::forward<Args>(args)...));
}

// void intrusive_ptr_add_ref(Frame* frame);
// void intrusive_ptr_release(Frame* frame);

}  // namespace hyrise