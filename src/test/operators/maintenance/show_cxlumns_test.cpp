#include <memory>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/maintenance/show_cxlumns.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

class ShowCxlumnsTest : public BaseTest {
 protected:
  void SetUp() override {
    auto t1 = load_table("src/test/tables/int_float_double_string.tbl", 2);
    auto t2 = load_table("src/test/tables/int_float_with_null.tbl", 2);

    StorageManager::get().add_table("int_float_double_string", t1);
    StorageManager::get().add_table("int_float_with_null", t2);
  }
};

TEST_F(ShowCxlumnsTest, OperatorName) {
  auto sc = std::make_shared<ShowCxlumns>("table_name");

  EXPECT_EQ(sc->name(), "ShowCxlumns");
}

TEST_F(ShowCxlumnsTest, CanBeCopied) {
  auto sc = std::make_shared<ShowCxlumns>("table_name");

  auto copied = sc->deep_copy();
  ASSERT_NE(nullptr, std::dynamic_pointer_cast<ShowCxlumns>(copied));
  ASSERT_NE(sc, copied) << "Copy returned the same object";
}

TEST_F(ShowCxlumnsTest, CanShowCxlumns) {
  auto sc = std::make_shared<ShowCxlumns>("int_float_double_string");
  sc->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/show_cxlumns/int_float_double_string.tbl", 3);
  EXPECT_TABLE_EQ_ORDERED(sc->get_output(), expected_result);
}

TEST_F(ShowCxlumnsTest, CanShowCxlumnsWithNull) {
  auto sc = std::make_shared<ShowCxlumns>("int_float_with_null");
  sc->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/show_cxlumns/int_float_with_null.tbl", 3);
  EXPECT_TABLE_EQ_ORDERED(sc->get_output(), expected_result);
}

TEST_F(ShowCxlumnsTest, NoCxlumns) {
  StorageManager::get().add_table("no_cxlumns", std::make_shared<Table>(TableCxlumnDefinitions{}, TableType::Data));

  auto sc = std::make_shared<ShowCxlumns>("no_cxlumns");
  sc->execute();

  auto out = sc->get_output();
  EXPECT_EQ(out->row_count(), 0u);
  EXPECT_EQ(out->cxlumn_count(), 3u);
}

}  // namespace opossum
