#include "buffer_managed_ptr.hpp"
#include "hyrise.hpp"
#include "storage/buffer/buffer_manager.hpp"


namespace hyrise {

BufferManager& get_buffer_manager() {
  return Hyrise::get().buffer_manager;
}

}  // namespace hyrise
