#include "hyrise.hpp"

namespace opossum {

enum Purpose { None, HashJoinHashTable, HashJoinMaterialization };

template <typename T, Purpose P>
class TrackingAllocator : public PolymorphicAllocator<T> {
 public:
  TrackingAllocator()
      : PolymorphicAllocator<T>(&(*Hyrise::get().memory_resource_manager.get_memory_resource(
            static_cast<std::string>(magic_enum::enum_name(P))))) {}
};

}  // namespace opossum