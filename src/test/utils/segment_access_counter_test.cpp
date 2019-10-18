#include "gtest/gtest.h"

#include <map>
#include <memory>

#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "utils/segment_access_counter.hpp"

namespace opossum {

TEST(SegmentAccessCounter, SegmentIDs) {
  ValueSegment<int32_t> vs1{false};
  auto id = vs1.id();
  ValueSegment<int32_t> vs2{false};
  ++id;
  EXPECT_EQ(id, vs2.id());
  ValueSegment<int32_t> vs3{false};
  ++id;
  EXPECT_EQ(id, vs3.id());
}

TEST(SegmentAccessCounter, ManualIncrease) {
  ValueSegment<int32_t> vs{false};
  EXPECT_EQ(0, SegmentAccessCounter::instance().statistics(vs.id()).count[SegmentAccessCounter::DirectRead]);
  SegmentAccessCounter::instance().increase(vs.id(), SegmentAccessCounter::DirectRead);
  EXPECT_EQ(1, SegmentAccessCounter::instance().statistics(vs.id()).count[SegmentAccessCounter::DirectRead]);
}

TEST(SegmentAccessCounter, ValueSegmentAppend) {
  ValueSegment<int32_t> vs{false};
  EXPECT_EQ(0, SegmentAccessCounter::instance().statistics(vs.id()).count[SegmentAccessCounter::Append]);
  vs.append(42);
  EXPECT_EQ(1, SegmentAccessCounter::instance().statistics(vs.id()).count[SegmentAccessCounter::Append]);
  vs.append(66);
  EXPECT_EQ(2, SegmentAccessCounter::instance().statistics(vs.id()).count[SegmentAccessCounter::Append]);
}

TEST(SegmentAccessCounter, ValueSegmentWithIterators) {
  ValueSegment<int32_t> vs{false};
  vs.append(42);
  vs.append(66);
  vs.append(666);
  EXPECT_EQ(3, SegmentAccessCounter::instance().statistics(vs.id()).count[SegmentAccessCounter::Append]);

  const auto iterable = ValueSegmentIterable{vs};
  iterable.for_each([](const auto& value) { /* do nothing. We just want to increase the access counter */ });
  EXPECT_EQ(3, SegmentAccessCounter::instance().statistics(vs.id()).count[SegmentAccessCounter::IteratorAccess]);
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

  SegmentAccessCounter::instance().save_to_csv(tables, "segment_access_statistics_test.csv");
}


}  // namespace opossum