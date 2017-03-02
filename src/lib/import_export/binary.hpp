#pragma once

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {
namespace binary {

enum class ColumnType : uint8_t { value_column = 0, dictionary_column = 1 };

}  // namespace binary
}  // namespace opossum
