#include "gtest/gtest.h"

#include "static_hash_map.hpp"

namespace opossum {

class StaticHashMapTest : public ::testing::Test {
 public:
  struct IdentityHash {
    template<typename T>
    size_t operator()(const T& value) const {
      return static_cast<size_t>(value);
    }
  };
};

TEST_F(StaticHashMapTest, Basic) {
  auto map = StaticHashMap<int32_t, int64_t, IdentityHash>{32};

  EXPECT_EQ(map.size(), 0u);
  EXPECT_EQ(map.load_factor(), 0.0f);
  EXPECT_EQ(map.find(1), map.end());

  map.insert({int32_t{99}, int64_t{3}});

  ASSERT_NE(map.find(99), map.end());
  EXPECT_EQ(*map.find(99), std::make_pair(int32_t{99}, int64_t{3}));
  EXPECT_EQ(map.size(), 1u);
  EXPECT_EQ(map.load_factor(), 1.0f / 32.0f);

  map.insert({int32_t{999}, int64_t{1}});
  map.insert({int32_t{102}, int64_t{2}});
  // 1 and 33 have the same masked hash
  map.insert({int32_t{33}, int64_t{4}});
  map.insert({int32_t{1}, int64_t{8}});

  EXPECT_EQ(map.size(), 5u);
  EXPECT_EQ(map.load_factor(), 5.0f / 32.0f);
  ASSERT_NE(map.find(999), map.end());
  EXPECT_EQ(*map.find(999), std::make_pair(int32_t{999}, int64_t{1}));
  ASSERT_NE(map.find(102), map.end());
  EXPECT_EQ(*map.find(102), std::make_pair(int32_t{102}, int64_t{2}));
  ASSERT_NE(map.find(33), map.end());
  EXPECT_EQ(*map.find(33), std::make_pair(int32_t{33}, int64_t{4}));
  ASSERT_NE(map.find(1), map.end());
  EXPECT_EQ(*map.find(1), std::make_pair(int32_t{1}, int64_t{8}));

  EXPECT_EQ(map.find(55), map.end());
  EXPECT_EQ(map.find(600), map.end());
}

}  // namespace opossum

