#include "gtest/gtest.h"

#include <map>
#include <memory>

#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/segment_access_statistics.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"


namespace opossum {

TEST(SegmentAccessCounter, ManualIncrease) {
  ValueSegment<int32_t> vs{false};
  EXPECT_EQ(0, vs.access_statistics().count(SegmentAccessType::Other));
  vs.access_statistics().on_other_access(1);
  EXPECT_EQ(1, vs.access_statistics().count(SegmentAccessType::Other));
}

TEST(SegmentAccessCounter, ValueSegmentAppend) {
  ValueSegment<int32_t> vs{false};
  EXPECT_EQ(0, vs.access_statistics().count(SegmentAccessType::Other));
  vs.append(42);
  EXPECT_EQ(1, vs.access_statistics().count(SegmentAccessType::Other));
  vs.append(66);
  EXPECT_EQ(2, vs.access_statistics().count(SegmentAccessType::Other));
}

TEST(SegmentAccessCounter, ValueSegmentWithIterators) {
  ValueSegment<int32_t> vs{false};
  vs.append(42);
  vs.append(66);
  vs.append(666);
  EXPECT_EQ(3, vs.access_statistics().count(SegmentAccessType::Other));

  const auto iterable = ValueSegmentIterable{vs};
  iterable.for_each([](const auto& value) { /* do nothing. We just want to increase the access counter */ });
  EXPECT_EQ(3, vs.access_statistics().count(SegmentAccessType::IteratorAccess));
}

TEST(SegmentAccessCounter, ExportStatistics) {
  std::map<std::string, std::shared_ptr<Table>> tables;
  auto table_ptr = std::make_shared<Table>(
      TableColumnDefinitions{TableColumnDefinition{"zip", DataType::Int, false},
                             TableColumnDefinition{"city", DataType::String, false}},
      TableType::Data);

  table_ptr->append({14480, "Potsdam"});
  table_ptr->append({30625, "Hannover"});
  table_ptr->append({49076, "Osnabrück"});

  tables["addresses"] = std::move(table_ptr);

  SegmentAccessStatistics_T::save_to_csv(tables, "segment_access_statistics_test.csv");
}

TEST(SegmentAccessCounter, Reset) {
  ValueSegment<int32_t> vs{false};
  vs.append(42);
  vs.append(66);
  vs.append(666);
  EXPECT_EQ(3, vs.access_statistics().count(SegmentAccessType::Other));
  vs.access_statistics().reset();
  EXPECT_EQ(0, vs.access_statistics().count(SegmentAccessType::Other));
}


}  // namespace opossum