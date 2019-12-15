#include "base_test.hpp"

#include "hyrise.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "synthetic_table_generator.hpp"

namespace opossum {

TEST(SyntheticTableGeneratorTest, StringGeneration) {
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(0), "          ");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(1), "         1");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(2), "         2");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(17), "         H");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(117), "        1t");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(50'018), "       D0k");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(3'433'820), "      EPIC");

  // Negative values are not supported.
  ASSERT_THROW(SyntheticTableGenerator::generate_value<pmr_string>(-17), std::logic_error);
}

TEST(SyntheticTableGeneratorTest, ThrowOnParameterLengthMismatch) {
  auto table_generator = std::make_shared<SyntheticTableGenerator>();
  const auto uniform_distribution = ColumnDataDistribution::make_uniform_config(0.0, 1.0);

  // vectors storing the column properties are expected to have the same length
  ASSERT_THROW(table_generator->generate_table({uniform_distribution, uniform_distribution}, {DataType::Int}, 15, 10),
               std::logic_error);
  ASSERT_THROW(table_generator->generate_table({uniform_distribution}, {DataType::Int, DataType::Int}, 15, 10),
               std::logic_error);
}

TEST(SyntheticTableGeneratorTest, TestGeneratedValueRange) {
  constexpr auto row_count = size_t{100};
  constexpr auto chunk_size = size_t{10};
  auto table_generator = std::make_shared<SyntheticTableGenerator>();
  auto uniform_distribution_0_1 = ColumnDataDistribution::make_uniform_config(0.0, 1.0);

  auto table = table_generator->generate_table({uniform_distribution_0_1}, {DataType::Double}, row_count, chunk_size,
                                               {{EncodingType::Dictionary}});
  for (auto table_row_id = size_t{0}; table_row_id < 100; ++table_row_id) {
    const auto value = table->get_value<double>(ColumnID{0}, table_row_id);
    ASSERT_TRUE(value >= 0.0 && value <= 1.0);
  }

  EXPECT_EQ(table->row_count(), row_count);
  EXPECT_EQ(table->chunk_count(), row_count / chunk_size);
}

using Params = std::tuple<DataType, ColumnDataDistribution>;

class SyntheticTableGeneratorDataTypeTests : public testing::TestWithParam<Params> {};

TEST_P(SyntheticTableGeneratorDataTypeTests, IntegerTable) {
  constexpr auto row_count = size_t{25};
  constexpr auto chunk_size = size_t{10};

  const auto tested_data_type = std::get<0>(GetParam());
  auto table_generator = std::make_shared<SyntheticTableGenerator>();

  std::vector<SegmentEncodingSpec> supported_segment_encodings;
  auto replace_unsupporting_encoding_types = [&](SegmentEncodingSpec spec) {
    if (encoding_supports_data_type(spec.encoding_type, tested_data_type)) {
      return spec;
    }
    return SegmentEncodingSpec{EncodingType::Unencoded};
  };
  std::transform(all_segment_encoding_specs.begin(), all_segment_encoding_specs.end(),
                 std::back_inserter(supported_segment_encodings), replace_unsupporting_encoding_types);

  const auto test_data_types = std::vector<DataType>(supported_segment_encodings.size(), tested_data_type);
  const auto test_data_distributions =
      std::vector<ColumnDataDistribution>(supported_segment_encodings.size(), std::get<1>(GetParam()));
  const auto column_names = std::vector<std::string>(supported_segment_encodings.size(), "column_name");

  auto table = table_generator->generate_table(test_data_distributions, test_data_types, row_count, chunk_size,
                                               supported_segment_encodings, column_names);

  const auto generated_chunk_count = table->chunk_count();
  const auto generated_column_count = table->column_count();
  EXPECT_EQ(table->row_count(), row_count);
  EXPECT_EQ(generated_chunk_count, static_cast<size_t>(std::round(static_cast<float>(row_count) / chunk_size)));
  EXPECT_EQ(generated_column_count, supported_segment_encodings.size());

  for (auto column_id = ColumnID{0}; column_id < generated_column_count; ++column_id) {
    EXPECT_EQ(table->column_data_type(column_id), tested_data_type);
    EXPECT_EQ(table->column_name(column_id), "column_name");
  }

  for (auto chunk_id = ChunkID{0}; chunk_id < generated_chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    BaseTest::assert_chunk_encoding(chunk, supported_segment_encodings);
  }
}

auto formatter = [](const testing::TestParamInfo<Params> info) {
  auto stream = std::stringstream{};
  switch (std::get<1>(info.param).distribution_type) {
    case DataDistributionType::Uniform:
      stream << "Uniform";
      break;
    case DataDistributionType::Pareto:
      stream << "Pareto";
      break;
    case DataDistributionType::NormalSkewed:
      stream << "Skewed";
  }

  stream << "_" << data_type_to_string.left.at(std::get<0>(info.param));
  return stream.str();
};

// For the skewed distribution, we use a location of 1,000 to move the distribution far into the positive number range.
// The reason is that string values cannot be generated for negative values.
INSTANTIATE_TEST_SUITE_P(SyntheticTableGeneratorDataType, SyntheticTableGeneratorDataTypeTests,
                         testing::Combine(testing::Values(DataType::Int, DataType::Long, DataType::Float,
                                                          DataType::Double, DataType::String),
                                          testing::Values(ColumnDataDistribution::make_uniform_config(0.0, 10'000),
                                                          ColumnDataDistribution::make_pareto_config(),
                                                          ColumnDataDistribution::make_skewed_normal_config(1'000.0))),
                         formatter);
}  // namespace opossum
