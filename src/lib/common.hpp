#pragma once

#include <cstddef>
#include <string>

#define DEV_ONLY
// #define DEV_ONLY __attribute__ ((deprecated))
// TODO decide how we want to deal with functions that should not be used in performance-critical code

namespace opossum {
	using std::to_string;
}