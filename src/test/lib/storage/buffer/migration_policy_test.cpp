#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/migration_policy.hpp"

namespace hyrise {

class MigrationPolicyTest : public BaseTest {
 public:
  static constexpr auto iterations = 200;
};

TEST_F(MigrationPolicyTest, TestLazyMigrationPolicy) {
  const auto seed = 1648481924;
  const auto policy = LazyMigrationPolicy{seed};
  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_dram_during_read() == true;
    }
    EXPECT_EQ(bypass_count, 199);
  }

  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_dram_during_write() == true;
    }
    EXPECT_EQ(bypass_count, 198);
  }

  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_numa_during_read() == true;
    }
    EXPECT_EQ(bypass_count, 148);
  }

  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_numa_during_write() == true;
    }
    EXPECT_EQ(bypass_count, 0);
  }
}

TEST_F(MigrationPolicyTest, TestEagerMigrationPolicy) {
  const auto seed = 1477473;
  const auto policy = EagerMigrationPolicy{seed};
  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_dram_during_read() == true;
    }
    EXPECT_EQ(bypass_count, 0);
  }

  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_dram_during_write() == true;
    }
    EXPECT_EQ(bypass_count, 0);
  }

  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_numa_during_read() == true;
    }
    EXPECT_EQ(bypass_count, 0);
  }

  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_numa_during_write() == true;
    }
    EXPECT_EQ(bypass_count, 0);
  }
}

TEST_F(MigrationPolicyTest, TestCustomMigrationPolicy) {
  const auto seed = 1648102924;
  const auto policy = MigrationPolicy{0.13, 0.3, 0.6, 0.9, seed};
  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_dram_during_read() == true;
    }
    EXPECT_EQ(bypass_count, 174);
  }

  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_dram_during_write() == true;
    }
    EXPECT_EQ(bypass_count, 143);
  }

  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_numa_during_read() == true;
    }
    EXPECT_EQ(bypass_count, 79);
  }

  {
    auto bypass_count = size_t{0};
    for (auto i = 0; i < iterations; ++i) {
      bypass_count += policy.bypass_numa_during_write() == true;
    }
    EXPECT_EQ(bypass_count, 15);
  }
}

}  // namespace hyrise