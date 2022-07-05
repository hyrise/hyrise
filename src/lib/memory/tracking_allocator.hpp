#include "hyrise.hpp"

namespace opossum {

enum Purpose { None, HashJoinHashTable, HashJoinMaterialization };

template <typename T>
class TrackingAllocator : public PolymorphicAllocator<T> {
 public:
  TrackingAllocator()
      : PolymorphicAllocator<T>(&(*Hyrise::get().memory_resource_manager.get_memory_resource(
            "test"))) {}
};

}  // namespace opossum