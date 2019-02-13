#include "base_test.hpp"

#include "operators/table_sample.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class TableSampleTest : public BaseTestWithParam<size_t> {
 public:
  void SetUp() override {
    const auto table = load_table("resources/test_data/tbl/int_int5_with_null.tbl", 5);
    table_wrapper = std::make_shared<TableWrapper>(table);

    table_wrapper->execute();
  }

  std::shared_ptr<TableWrapper> table_wrapper;
};

TEST_P(TableSampleTest, Sample) {
  const auto table = load_table("resources/test_data/tbl/int_int5_with_null.tbl", 5);
  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  const auto table_sample = std::make_shared<TableSample>(table_wrapper, GetParam());
  table_sample->execute();

  const auto expected_table_path = "resources/test_data/tbl/table_sample/int_int5_with_null_sample_" + std::to_string(GetParam()) + ".tbl";

  EXPECT_TABLE_EQ_ORDERED(table_sample->get_output(), load_table(expected_table_path));
}

INSTANTIATE_TEST_CASE_P(TableSampleTestInstantiation,
  TableSampleTest,
  ::testing::Values(0, 1, 4, 5, 16, 17), );  // NOLINT

}  // namespace opossum
