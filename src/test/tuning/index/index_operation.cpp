#include "../../base_test.hpp"

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "tuning/index/column_ref.hpp"
#include "tuning/index/index_operation.hpp"

namespace opossum {

class IndexOperationTest : public BaseTest {
 protected:
  void SetUp() override {
    auto table = std::make_shared<Table>();
    table->add_column("column_name", DataType::Int);
    StorageManager::get().add_table("table_name", table);
  }

  void TearDown() override { StorageManager::get().drop_table("table_name"); }

  ColumnRef _column_ref{"table_name", ColumnID{0}};
};

TEST_F(IndexOperationTest, GetColumRefTest) {
  IndexOperation operation{_column_ref, ColumnIndexType::GroupKey, true};
  EXPECT_EQ(operation.column(), _column_ref);
}

TEST_F(IndexOperationTest, GetColumnIndexTypeTest) {
  IndexOperation operation{_column_ref, ColumnIndexType::GroupKey, true};
  EXPECT_EQ(operation.type(), ColumnIndexType::GroupKey);
}

TEST_F(IndexOperationTest, GetCreateTest) {
  IndexOperation operation{_column_ref, ColumnIndexType::GroupKey, true};
  EXPECT_EQ(operation.create(), true);
}

TEST_F(IndexOperationTest, PrintOnStreamTest) {
  IndexOperation operation{_column_ref, ColumnIndexType::GroupKey, true};

  std::stringstream stream;
  operation.print_on(stream);

  std::string result = stream.str();
  EXPECT_EQ(result, "IndexOperation{Create on table_name.(column_name)}");
}

// ToDo(group01): add tests for actual index creation

}  // namespace opossum
