#ifdef HYRISE_WITH_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "base_test.hpp"

namespace hyrise {

class JemallocTest : public BaseTest {};


/**
 * We test for the exact version of jemalloc. We found that performance can vary significantly between jemalloc 
 * versions (see https://github.com/hyrise/hyrise/discussions/2628). This test is supposed to ensure that jemalloc is
 * not blindly updated. When updating jemalloc, please evaluate multiple CPU architectures using benchmark_all.sh.
 * For jemalloc v5.3, we use the option `oversize_threshold:0`. This might be no longer necessary with upcoming 
 * releases (see https://github.com/jemalloc/jemalloc/issues/2495).
 */
TEST_F(JemallocTest, VersionAndConfig) {
#ifndef HYRISE_WITH_JEMALLOC
  GTEST_SKIP();
#endif

  // Hmm ... what's the issue with the version?
  //
  const char* version;
  auto version_size = sizeof(version);
  const auto return_code_version = mallctl("version", &version, &version_size, nullptr, 0);
  EXPECT_EQ(return_code_version, 0);
  std::cout << "Version: " << version << std::endl;
  EXPECT_EQ(version, "5.3.0");

  auto oversize_threshold_setting = std::numeric_limits<size_t>::max();
  auto oversize_threshold_setting_size = sizeof(oversize_threshold_setting);
  const auto return_code_threshold = mallctl("opt.oversize_threshold", &oversize_threshold_setting,
		                             &oversize_threshold_setting_size, nullptr, 0);
  EXPECT_EQ(return_code_threshold, 0);
  EXPECT_EQ(oversize_threshold_setting, 0);
}

}  // namespace hyrise
