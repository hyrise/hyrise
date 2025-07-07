#include "base_test.hpp"
#include "utils/key_normalizer.h"

namespace hyrise {
class KeyNormalizerTest : public BaseTest {};

int compare_keys(const std::vector<unsigned char>& key_a, const std::vector<unsigned char>& key_b) {
  assert(key_a.size() == key_b.size() && "Keys must have the same length for comparison.");
  return std::memcmp(key_a.data(), key_b.data(), key_a.size());
}

TEST_F(KeyNormalizerTest, TestIntegerNormalization) {
  // ASC order
  {
    std::vector<unsigned char> key_5, key_10;
    KeyNormalizer norm_5(key_5), norm_10(key_10);

    norm_5.append(std::optional(5), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm_10.append(std::optional(10), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    ASSERT_LT(compare_keys(key_5, key_10), 0);
  }

  // DESC order
  {
    std::vector<unsigned char> key_5, key_10;
    KeyNormalizer norm_5(key_5), norm_10(key_10);
    norm_5.append(std::optional(5), NormalizedSortMode::Descending, NullsMode::NullsFirst);
    norm_10.append(std::optional(10), NormalizedSortMode::Descending, NullsMode::NullsFirst);
    ASSERT_GT(compare_keys(key_5, key_10), 0);
  }

  // Negative numbers (ASC)
  {
    std::vector<unsigned char> key_neg_10, key_5;
    KeyNormalizer norm_neg_10(key_neg_10), norm_5(key_5);
    norm_neg_10.append(std::optional(-10), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm_5.append(std::optional(5), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    ASSERT_LT(compare_keys(key_neg_10, key_5), 0);
  }
}

TEST_F(KeyNormalizerTest, TestStringNormalization) {
  // ASC order
  {
    std::vector<unsigned char> key_apple, key_orange;
    KeyNormalizer norm_apple(key_apple), norm_orange(key_orange);
    norm_apple.append(std::optional<pmr_string>("apple"), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm_orange.append(std::optional<pmr_string>("orange"), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    ASSERT_LT(compare_keys(key_apple, key_orange), 0);
  }

  // DESC order
  {
    std::vector<unsigned char> key_apple, key_orange;
    KeyNormalizer norm_apple(key_apple), norm_orange(key_orange);
    norm_apple.append(std::optional<pmr_string>("apple"), NormalizedSortMode::Descending, NullsMode::NullsFirst);
    norm_orange.append(std::optional<pmr_string>("orange"), NormalizedSortMode::Descending, NullsMode::NullsFirst);
    ASSERT_GT(compare_keys(key_apple, key_orange), 0);
  }

  // Prefix check
  {
    std::vector<unsigned char> key_pie, key_strudel;
    KeyNormalizer norm_pie(key_pie), norm_strudel(key_strudel);
    norm_pie.append(std::optional<pmr_string>("apple_pie"), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm_strudel.append(std::optional<pmr_string>("apple_strudel"), NormalizedSortMode::Ascending,
                        NullsMode::NullsFirst);
    ASSERT_LT(compare_keys(key_pie, key_strudel), 0);
  }
}

TEST_F(KeyNormalizerTest, TestFloatNormalization) {
  // ASC order
  {
    std::vector<unsigned char> key_3_14, key_9_99;
    KeyNormalizer norm_3_14(key_3_14), norm_9_99(key_9_99);
    norm_3_14.append(std::optional(3.14f), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm_9_99.append(std::optional(9.99f), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    ASSERT_LT(compare_keys(key_3_14, key_9_99), 0);
  }

  // DESC order
  {
    std::vector<unsigned char> key_3_14, key_9_99;
    KeyNormalizer norm_3_14(key_3_14), norm_9_99(key_9_99);
    norm_3_14.append(std::optional(3.14f), NormalizedSortMode::Descending, NullsMode::NullsFirst);
    norm_9_99.append(std::optional(9.99f), NormalizedSortMode::Descending, NullsMode::NullsFirst);
    ASSERT_GT(compare_keys(key_3_14, key_9_99), 0);
  }

  // Negative floats
  {
    std::vector<unsigned char> key_neg, key_pos;
    KeyNormalizer norm_neg(key_neg), norm_pos(key_pos);
    norm_neg.append(std::optional(-5.0f), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm_pos.append(std::optional(5.0f), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    ASSERT_LT(compare_keys(key_neg, key_pos), 0);
  }
}

TEST_F(KeyNormalizerTest, TestNullHandling) {
  // NULLS FIRST (ASC)
  {
    std::vector<unsigned char> key_null, key_10;
    KeyNormalizer norm_null(key_null), norm_10(key_10);
    norm_null.append(std::optional<int32_t>(), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm_10.append(std::optional(10), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    ASSERT_LT(compare_keys(key_null, key_10), 0);
  }

  // NULLS LAST (ASC)
  {
    std::vector<unsigned char> key_null, key_10;
    KeyNormalizer norm_null(key_null), norm_10(key_10);
    norm_null.append(std::optional<int32_t>(), NormalizedSortMode::Ascending, NullsMode::NullsLast);
    norm_10.append(std::optional(10), NormalizedSortMode::Ascending, NullsMode::NullsLast);
    ASSERT_GT(compare_keys(key_null, key_10), 0);
  }

  // NULLS FIRST (DESC)
  {
    std::vector<unsigned char> key_null, key_10;
    KeyNormalizer norm_null(key_null), norm_10(key_10);
    norm_null.append(std::optional<int32_t>(), NormalizedSortMode::Descending, NullsMode::NullsFirst);
    norm_10.append(std::optional(10), NormalizedSortMode::Descending, NullsMode::NullsFirst);
    ASSERT_LT(compare_keys(key_null, key_10), 0);
  }

  // NULLS LAST (DESC)
  {
    std::vector<unsigned char> key_null, key_10;
    KeyNormalizer norm_null(key_null), norm_10(key_10);
    norm_null.append(std::optional<int32_t>(), NormalizedSortMode::Descending, NullsMode::NullsLast);
    norm_10.append(std::optional(10), NormalizedSortMode::Descending, NullsMode::NullsLast);
    ASSERT_GT(compare_keys(key_null, key_10), 0);
  }
}

TEST_F(KeyNormalizerTest, TestComposedKeyNormalization) {
  // NULLS FIRST (ASC)
  // Key1: (10, "apple"), Key2: (10, "orange")
  {
    std::vector<unsigned char> key1, key2;
    KeyNormalizer norm1(key1), norm2(key2);

    norm1.append(std::optional(10), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm1.append(std::optional<pmr_string>("apple"), NormalizedSortMode::Ascending, NullsMode::NullsFirst);

    norm2.append(std::optional(10), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm2.append(std::optional<pmr_string>("orange"), NormalizedSortMode::Ascending, NullsMode::NullsFirst);

    // the size of the keys should be 18 bytes, the reason for this is: since we use NullsFirst ordering,
    // we have to reserve one byte to indicate this, the int32_t takes 4 bytes. Total will be 1 + 4 = 5
    // The string has a default prefix_size of 12 bytes, it also follows NullsFirst, so total is 1 + 12 = 13
    // to final size of the normalized key is 5 + 13 = 18
    ASSERT_EQ(key1.size(), 18);
    ASSERT_LT(compare_keys(key1, key2), 0);
  }

  // Key1: (5, "zulu"), Key2: (10, "alpha")
  {
    std::vector<unsigned char> key1, key2;
    KeyNormalizer norm1(key1), norm2(key2);

    norm1.append(std::optional(5), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm1.append(std::optional<pmr_string>("zulu"), NormalizedSortMode::Ascending, NullsMode::NullsFirst);

    norm2.append(std::optional(10), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm2.append(std::optional<pmr_string>("alpha"), NormalizedSortMode::Ascending, NullsMode::NullsFirst);

    ASSERT_LT(compare_keys(key1, key2), 0);
  }

  // Key1: (10, "apple", 9.99), Key2: (10, "apple", 3.14) DESC on float
  {
    std::vector<unsigned char> key1, key2;
    KeyNormalizer norm1(key1), norm2(key2);

    norm1.append(std::optional(10), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm1.append(std::optional<pmr_string>("apple"), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm1.append(std::optional(9.99f), NormalizedSortMode::Descending, NullsMode::NullsFirst);

    norm2.append(std::optional(10), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm2.append(std::optional<pmr_string>("apple"), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    norm2.append(std::optional(3.14f), NormalizedSortMode::Descending, NullsMode::NullsFirst);

    ASSERT_EQ(key1.size(), 23);  // (int_32 + null) + (string + null) + (float + null) = 23.
    ASSERT_LT(compare_keys(key1, key2), 0);
  }
}
}  // namespace hyrise
