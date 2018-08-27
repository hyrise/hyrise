#pragma once

#define EXPECT_CXLUMN_STATISTICS_IMPL(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_,       \
                                      data_type_, data_type_type, min_max_cmp)                                      \
  {                                                                                                                 \
    const auto cxlumn_statistics = cxlumn_statistics_expr;                                                          \
    ASSERT_EQ(cxlumn_statistics->data_type(), data_type_);                                                          \
    EXPECT_FLOAT_EQ(cxlumn_statistics->null_value_ratio(), null_value_ratio_);                                      \
    EXPECT_FLOAT_EQ(cxlumn_statistics->distinct_count(), distinct_count_);                                          \
    min_max_cmp(std::dynamic_pointer_cast<const CxlumnStatistics<data_type_type>>(cxlumn_statistics)->min(), min_); \
    min_max_cmp(std::dynamic_pointer_cast<const CxlumnStatistics<data_type_type>>(cxlumn_statistics)->max(), max_); \
  }

#define EXPECT_INT32_CXLUMN_STATISTICS(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_)         \
  EXPECT_CXLUMN_STATISTICS_IMPL(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_, DataType::Int, \
                                int32_t, EXPECT_EQ)

#define EXPECT_INT64_CXLUMN_STATISTICS(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_) \
  EXPECT_CXLUMN_STATISTICS_IMPL(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_,        \
                                DataType::Long, int64_t, EXPECT_EQ)

#define EXPECT_FLOAT_CXLUMN_STATISTICS(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_) \
  EXPECT_CXLUMN_STATISTICS_IMPL(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_,        \
                                DataType::Float, float, EXPECT_FLOAT_EQ)

#define EXPECT_DOUBLE_CXLUMN_STATISTICS(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_) \
  EXPECT_CXLUMN_STATISTICS_IMPL(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_,         \
                                DataType::Double, double, EXPECT_FLOAT_EQ)

#define EXPECT_STRING_CXLUMN_STATISTICS(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_) \
  EXPECT_CXLUMN_STATISTICS_IMPL(cxlumn_statistics_expr, null_value_ratio_, distinct_count_, min_, max_,         \
                                DataType::String, std::string, EXPECT_EQ)
