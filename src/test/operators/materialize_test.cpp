#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/materialize.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"

namespace opossum {
class MaterializeTest : public BaseTest {
 protected:
  virtual void SetUp() {
    _table = load_table("src/test/tables/int_float_w_null_8_rows.tbl", 3);

    _table_wrapper = std::make_shared<TableWrapper>(_table);
    _table_wrapper->execute();
  }

  std::shared_ptr<Table> _table;
  std::shared_ptr<TableWrapper> _table_wrapper;
};

TEST_F(MaterializeTest, MaterializeTable) {
  auto materialize = std::make_shared<Materialize>(_table_wrapper);
  materialize->execute();

  EXPECT_TABLE_EQ_ORDERED(materialize->get_output(), _table);
}

}  // namespace opossum
