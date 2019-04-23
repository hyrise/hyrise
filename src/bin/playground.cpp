#include <iostream>

extern "C" {
#include "../../../../third_party/tpcds-dbgen/config.h"
#include <porting.h>
#include <s_brand.h>
}

#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  for (int i = 0; i < 100; ++i) {
    auto brand = S_BRAND_TBL{};
    mk_s_brand(&brand, i);
    std::cout << "done" << std::endl;
  }
}
