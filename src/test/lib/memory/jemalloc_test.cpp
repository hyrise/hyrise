#include <limits>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#ifdef HYRISE_WITH_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "base_test.hpp"

namespace hyrise {

class JemallocTest : public BaseTest {};


/**
 * We test for the exact version of jemalloc. We found that performance can vary significantly between jemalloc 
 * versions (see https://github.com/hyrise/hyrise/discussions/2628). This test is supposed to motivate re-evaluating
 * the configuration of jemalloc. When updating jemalloc, please evaluate multiple CPU architectures using
 * `benchmark_all.sh`. For jemalloc v5.3, we use the option `oversize_threshold:0`. This might be no longer necessary
 * with upcoming releases (see https://github.com/jemalloc/jemalloc/issues/2495).
 */
TEST_F(JemallocTest, ConfigurationSettings) {
#ifndef HYRISE_WITH_JEMALLOC
  GTEST_SKIP();
#endif

  auto expected_settings = std::vector<std::pair<std::string, std::variant<bool, size_t, unsigned, std::string>>>{{"opt.oversize_threshold", size_t{0}},
                                                                                                  {"opt.dirty_decay_ms", size_t{20'000}},
                                                                                                  {"opt.dirty_decay_ms", size_t{20'000}},
                                                                                                  {"opt.narenas", unsigned{12}},
                                                                                                  {"opt.background_thread", true},
                                                                                                  {"version", std::string{"5.3.0-0-g54eaed1d8b56b1aa528be3bdd1877e59c56fa90c"}},
{"opt.percpu_arena", std::string{"percpu"}},
{"opt.metadata_thp", std::string{"auto"}}};
  
    //set(JEMALLOC_CONFIG "--with-malloc-conf=\"oversize_threshold:0,background_thread:true,percpu_arena:percpu,metadata_thp:auto,muzzy_decay_ms:20000,dirty_decay_ms:20000\"")
  for (const auto& [setting_name, setting_value] : expected_settings) {
    std::cerr << setting_name << std::endl;
    if (std::holds_alternative<size_t>(setting_value)) {
      auto setting = std::numeric_limits<size_t>::max();
      auto setting_size = sizeof(setting);
      const auto return_code = mallctl(setting_name.c_str(), &setting, &setting_size, nullptr, 0);
      EXPECT_EQ(return_code, 0);
      EXPECT_EQ(setting, std::get<size_t>(setting_value));
    } else if (std::holds_alternative<unsigned>(setting_value)) {
      auto setting = std::numeric_limits<unsigned>::max();
      auto setting_size = sizeof(setting);
      const auto return_code = mallctl(setting_name.c_str(), &setting, &setting_size, nullptr, 0);
      EXPECT_EQ(return_code, 0);
      EXPECT_EQ(setting, std::get<unsigned>(setting_value));
    } else if (std::holds_alternative<std::string>(setting_value)) {
      const char* str_setting;
      auto str_setting_size = sizeof(str_setting);
      const auto return_code = mallctl(setting_name.c_str(), &str_setting, &str_setting_size, nullptr, 0);
      EXPECT_EQ(return_code, 0);
      EXPECT_EQ(str_setting, std::get<std::string>(setting_value));
    } else {
      Assert(std::holds_alternative<bool>(setting_value), "Expected bool value.");

      auto setting = false;
      auto setting_size = sizeof(setting);
      const auto return_code = mallctl(setting_name.c_str(), &setting, &setting_size, nullptr, 0);
      EXPECT_EQ(return_code, 0);
      EXPECT_EQ(setting, std::get<bool>(setting_value));
    }
  }

  auto oversize_threshold_setting = std::numeric_limits<size_t>::max();
  auto oversize_threshold_setting_size = sizeof(oversize_threshold_setting);
  const auto return_code_threshold = mallctl("opt.oversize_threshold", &oversize_threshold_setting,
		                             &oversize_threshold_setting_size, nullptr, 0);
  EXPECT_EQ(return_code_threshold, 0);
  EXPECT_EQ(oversize_threshold_setting, 0);

}

}  // namespace hyrise
