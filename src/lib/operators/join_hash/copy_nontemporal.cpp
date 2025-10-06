#include "copy_nontemporal.hpp"
#include "hwy/highway.h"

HWY_BEFORE_NAMESPACE();

namespace highway {
using namespace hwy::HWY_NAMESPACE;

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"

void copy_nontemporal(const uint8_t* HWY_RESTRICT in_ptr, size_t count, uint8_t* HWY_RESTRICT out_ptr) {
  const auto tag = ScalableTag<uint8_t>{};
  for (auto i = size_t{0}; i <= count; i += Lanes(tag)) {
    const auto vec = Load(tag, in_ptr + i);
    Stream(vec, tag, out_ptr + i);
  }
}
#pragma clang diagnostic pop

}  // namespace highway
HWY_AFTER_NAMESPACE();
