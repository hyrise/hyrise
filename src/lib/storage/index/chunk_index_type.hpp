#pragma once

#include <cstdint>

namespace hyrise {

enum class ChunkIndexType : uint8_t { GroupKey, CompositeGroupKey, AdaptiveRadixTree };

}  // namespace hyrise
