#pragma once

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

enum class BinaryColumnType : uint8_t { value_column = 0, dictionary_column = 1 };

}  // namespace opossum
