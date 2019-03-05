#pragma once

#define EXPECT_COLUMN_STATISTICS_IMPL(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_,       \
                                      data_type_, data_type_type, min_max_cmp)                                      \
  {                                                                                                                 \
    const auto column_statistics = column_statistics_expr;                                                          \
    ASSERT_EQ(column_statistics->data_type(), data_type_);                                                          \
    EXPECT_FLOAT_EQ(column_statistics->null_value_ratio(), null_value_ratio_);                                      \
    EXPECT_FLOAT_EQ(column_statistics->distinct_count(), distinct_count_);                                          \
    min_max_cmp(std::dynamic_pointer_cast<const ColumnStatistics<data_type_type>>(column_statistics)->min(), min_); \
    min_max_cmp(std::dynamic_pointer_cast<const ColumnStatistics<data_type_type>>(column_statistics)->max(), max_); \
  }

#define EXPECT_INT32_COLUMN_STATISTICS(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_)         \
  EXPECT_COLUMN_STATISTICS_IMPL(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_, DataType::Int, \
                                int32_t, EXPECT_EQ)

#define EXPECT_INT64_COLUMN_STATISTICS(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_) \
  EXPECT_COLUMN_STATISTICS_IMPL(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_,        \
                                DataType::Long, int64_t, EXPECT_EQ)

#define EXPECT_FLOAT_COLUMN_STATISTICS(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_) \
  EXPECT_COLUMN_STATISTICS_IMPL(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_,        \
                                DataType::Float, float, EXPECT_FLOAT_EQ)

#define EXPECT_DOUBLE_COLUMN_STATISTICS(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_) \
  EXPECT_COLUMN_STATISTICS_IMPL(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_,         \
                                DataType::Double, double, EXPECT_FLOAT_EQ)

#define EXPECT_STRING_COLUMN_STATISTICS(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_) \
  EXPECT_COLUMN_STATISTICS_IMPL(column_statistics_expr, null_value_ratio_, distinct_count_, min_, max_,         \
                                DataType::String, pmr_string, EXPECT_EQ)
