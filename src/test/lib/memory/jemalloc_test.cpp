#include <limits>
#include <string>
#include <thread>
#include <type_traits>
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
 * We test for the exact version of jemalloc and the used configuration. We found that performance can vary
 * significantly between jemallo versions (see https://github.com/hyrise/hyrise/discussions/2628). This test is
& * supposed to motivate re-evaluating
 * the configuration of jemalloc. When updating jemalloc, please evaluate multiple CPU architectures using
 * `benchmark_all.sh`. For jemalloc v5.3, we use the option `oversize_threshold:0`. This might be no longer necessary
 * with upcoming releases (see https://github.com/jemalloc/jemalloc/issues/2495).
 */
TEST_F(JemallocTest, ConfigurationSettings) {
#ifndef HYRISE_WITH_JEMALLOC
  GTEST_SKIP();
#endif

  const auto jemalloc_version = std::string{"5.3.0-0-g54eaed1d8b56b1aa528be3bdd1877e59c56fa90c"};
  const auto percpu_setting = std::string{"percu"};
  const auto metadata_thp_setting = std::string{"auto"};
  auto expected_settings = std::vector<std::pair<std::string, std::variant<bool, size_t, unsigned, const char*>>>{
      {"opt.oversize_threshold", size_t{0}},
      {"opt.dirty_decay_ms", size_t{20'000}},
      {"opt.dirty_decay_ms", size_t{20'000}},
      {"opt.narenas", unsigned{std::thread::hardware_concurrency()}},
      {"opt.background_thread", true},
      {"version", jemalloc_version.data()},
      {"opt.percpu_arena", percpu_setting.data()},
      {"opt.metadata_thp", metadata_thp_setting.data()}};

  for (const auto& [setting_name, setting_value] : expected_settings) {
    std::visit(
        [setting_name = setting_name, setting_value = setting_value](auto&& argument) {
          using T = std::decay_t<decltype(argument)>;
          auto setting = T{};

          if constexpr (std::is_same_v<T, bool>) {
            setting = false;
          } else if constexpr (std::is_integral_v<T>) {
            setting = std::numeric_limits<T>::max();
          }

          auto setting_size = sizeof(setting);
          const auto return_code = mallctl(setting_name.c_str(), &setting, &setting_size, nullptr, 0);
          EXPECT_EQ(return_code, 0);

          if constexpr (std::is_same_v<T, const char*>) {
            EXPECT_EQ(*setting, *std::get<T>(setting_value));
          } else {
            EXPECT_EQ(setting, std::get<T>(setting_value));
          }
        },
        setting_value);
  }
}

}  // namespace hyrise
