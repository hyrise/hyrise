#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {
class operators_projection : public ::testing::Test {
  virtual void SetUp() {
    test_table = std::make_shared<Table>(opossum::Table(2));

    test_table->add_column("a", "int");
    test_table->add_column("b", "float");

    test_table->append({123, 456.7f});
    test_table->append({1234, 457.7f});
    test_table->append({12345, 458.7f});

    opossum::StorageManager::get().add_table("table_a", std::move(test_table));

    gt = std::make_shared<GetTable>("table_a");
  }

 public:
  std::shared_ptr<opossum::Table> test_table;
  std::shared_ptr<opossum::GetTable> gt;
};

TEST_F(operators_projection, project_single_column) {
  std::vector<std::string> column_filter = {"a"};
  auto projection = std::make_shared<Projection>(gt, column_filter);
  projection->execute();

  EXPECT_EQ(projection->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection->get_output()->row_count(), gt->get_output()->row_count());
  EXPECT_THROW(projection->get_output()->get_column_id_by_name("b"), std::exception);
}

TEST_F(operators_projection, double_project_single_column) {
  std::vector<std::string> column_filter = {"a"};
  auto projection1 = std::make_shared<Projection>(gt, column_filter);
  projection1->execute();

  auto projection2 = std::make_shared<Projection>(projection1, column_filter);
  projection2->execute();

  EXPECT_EQ(projection2->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection2->get_output()->row_count(), gt->get_output()->row_count());
  EXPECT_THROW(projection2->get_output()->get_column_id_by_name("b"), std::exception);
}

TEST_F(operators_projection, project_all_columns) {
  std::vector<std::string> column_filter = {"a", "b"};
  auto projection = std::make_shared<Projection>(gt, column_filter);
  projection->execute();

  EXPECT_EQ(projection->get_output()->col_count(), gt->get_output()->col_count());
  EXPECT_EQ(projection->get_output()->row_count(), gt->get_output()->row_count());
  EXPECT_EQ(projection->get_output()->get_column_id_by_name("b"), (u_int)1);
}
}  // namespace opossum
