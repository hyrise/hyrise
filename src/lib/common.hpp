#pragma once

#include <cstddef>
#include <iostream>
#include <string>
#include <vector>

#define DEV_ONLY
// #define DEV_ONLY __attribute__ ((deprecated))
// TODO(Anyone): decide how we want to deal with functions that should not be used in performance-critical code - maybe
// DEV_ONLY
// can only be called from functions that are DEV_ONLY themselves

namespace opossum {
using std::to_string;
}
