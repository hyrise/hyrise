#include <gtest/gtest.h>

#include <bitset>
#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"
#include "operators/reduce.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace hyrise {

class OperatorsReduceTest : public BaseTest {
 protected:
  void SetUp() override {
    auto int_int_7 = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});

    _int_int = std::make_shared<TableWrapper>(std::move(int_int_7));
    _int_int->never_clear_output();
    _int_int->execute();

    auto int_int_extended = load_table("resources/test_data/tbl/int_int_shuffled_3.tbl", ChunkOffset{7});

    _int_int_extended = std::make_shared<TableWrapper>(std::move(int_int_extended));
    _int_int_extended->never_clear_output();
    _int_int_extended->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _int_int;
  std::shared_ptr<TableWrapper> _int_int_extended;
};

TEST_F(OperatorsReduceTest, ScanTableWithFilterCreatedOnItself) {
  const auto expected = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});
  auto reduce = std::make_shared<Reduce>(_int_int, ColumnID{0});
  reduce->execute();
  EXPECT_TABLE_EQ_UNORDERED(reduce->get_output(), expected);
}

TEST_F(OperatorsReduceTest, SimpleFilterTest) {
  auto reduce0 = std::make_shared<Reduce>(_int_int, ColumnID{0});
  reduce0->execute();

  auto reduce1 = std::make_shared<Reduce>(_int_int_extended, ColumnID{0});
  reduce1->import_filter(reduce0->export_filter());

  const auto expected = load_table("resources/test_data/tbl/int_int_shuffled_3_res.tbl", ChunkOffset{7});
  reduce1->execute();
  EXPECT_TABLE_EQ_UNORDERED(reduce1->get_output(), expected);
}

}  // namespace hyrise
