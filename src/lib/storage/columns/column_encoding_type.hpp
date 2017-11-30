#pragma once

#include <cstdint>


namespace opossum {

enum class ColumnEncodingType : uint8_t {
  Invalid,
  Dictionary
};

}  // namespace opossum
