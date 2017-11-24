#pragma once

#include <cstdint>

namespace opossum {

enum class ColumnIndexType : uint8_t { GroupKey, CompositeGroupKey, AdaptiveRadixTree };

}  // namespace opossum
