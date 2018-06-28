#include <memory>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/index/column_index_type.hpp"
#include "../lib/storage/index/index_info.hpp"
#include "../lib/types.hpp"

namespace opossum {

class IndexInfoTest : public BaseTest {};

TEST_F(IndexInfoTest, EqualityOperator) {
  IndexInfo a{std::vector<ColumnID>{ColumnID{0}}, "name", ColumnIndexType::Invalid};
  IndexInfo b{std::vector<ColumnID>{ColumnID{0}}, "name", ColumnIndexType::Invalid};

  EXPECT_EQ(a, b);

  b.column_ids[0] = ColumnID{1};
  EXPECT_NE(a, b);
  b.column_ids = a.column_ids;
  EXPECT_EQ(a, b);

  b.name = "other name";
  EXPECT_NE(a, b);
  b.name = a.name;
  EXPECT_EQ(a, b);

  b.type = ColumnIndexType::GroupKey;
  EXPECT_NE(a, b);
}
}  // namespace opossum
