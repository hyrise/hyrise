#pragma once
#include "hwy/base.h"  // HWY_RESTRICT

namespace highway {
void copy_nontemporal(const uint8_t* HWY_RESTRICT in_ptr, size_t count, uint8_t* HWY_RESTRICT out_ptr);
}  // namespace highway
