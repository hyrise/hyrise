#include <iostream>

#include <jemalloc/jemalloc.h>

#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  uint64_t epoch = 1;
  size_t  sz = sizeof(epoch);
  mallctl("epoch", &epoch, &sz, &epoch, sz);

  const char *version_str;
  size_t version_size = sizeof(version_str);
  auto c0 = mallctl("version", &version_str, &version_size, nullptr, 0);
  std::cout << c0 << " - version: " << version_str << std::endl;

  size_t bgt;
  sz = sizeof(bgt);
  auto c00 = mallctl("max_background_threads", &bgt, &sz, nullptr, 0);
  std::cout << c00 << " - background threads: " << bgt << std::endl;

  size_t ovth;
  auto c1 = mallctl("opt.oversize_threshold", &ovth, &sz, nullptr, 0);
  std::cout << c1 << " - oversize threshold: " << ovth << std::endl;

  return 0;
}
