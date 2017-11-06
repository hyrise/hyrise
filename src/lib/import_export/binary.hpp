#pragma once

namespace opossum {

enum class BinaryColumnType : uint8_t { value_column = 0, dictionary_column = 1 };

using BoolAsByteType = uint8_t;

}  // namespace opossum
