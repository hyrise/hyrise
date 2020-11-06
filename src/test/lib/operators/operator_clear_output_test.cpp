#include "base_test.hpp"

#include "operators/table_wrapper.hpp"

namespace opossum {

class OperatorClearOutputTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper_a = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));
    _table_wrapper_b = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float2.tbl", 2));
    _table_wrapper_c = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float3.tbl", 2));
    _table_wrapper_d = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int3.tbl", 2));

    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
    _table_wrapper_c->execute();
    _table_wrapper_d->execute();

    _test_table = load_table("resources/test_data/tbl/int_float.tbl", 2);
    Hyrise::get().storage_manager.add_table("aNiceTestTable", _test_table);
  }

  std::shared_ptr<Table> _test_table;
  std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_b, _table_wrapper_c, _table_wrapper_d;
};

TEST_F(OperatorClearOutputTest, ClearOutput) {

  // TODO

//  EXPECT_EQ(std::dynamic_pointer_cast<OperatorTask>(*task_it)->get_operator()->get_output(), nullptr);
}

}
