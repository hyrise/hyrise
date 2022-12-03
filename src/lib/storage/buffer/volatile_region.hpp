#pragma once

#include "frame.hpp"
#include "types.hpp"

namespace hyrise {
/**
 * @brief 
 * 
 * TODO: Might want to use a memory arena isnstead of
 */
class VolatileRegion : private Noncopyable {
 public:
  VolatileRegion(size_t num_bytes);
  ~VolatileRegion() = default;

  Frame allocate_frame();

 private:
  std::byte* _data;
};
}  // namespace hyrise