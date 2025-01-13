#include <gtest/gtest.h>

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
  }

 protected:
  std::shared_ptr<TableWrapper> _int_int;
};

TEST_F(OperatorsReduceTest, DoubleScan) {
  const auto expected = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});
  auto reduce = std::make_shared<Reduce>(_int_int);
  reduce->execute();
  EXPECT_TABLE_EQ_UNORDERED(reduce->get_output(), expected);
}

}  // namespace hyrise
