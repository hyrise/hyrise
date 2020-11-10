#include <fstream>

#include "micro_benchmark_basic_fixture.hpp"

#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/index_scan.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_scan/column_like_table_scan_impl.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/index/group_key/group_key_index.hpp"

#include "utils/load_table.hpp"
#include "operators/print.hpp"
#include "operators/sort.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {
using namespace opossum;

///////////////////////////////
// benchmark seetings
///////////////////////////////

// input and output settings 
///////////////////////////////
constexpr auto SEGMENT_META_DATA_FILE = "../../out/timestamp/segment_meta_data_int_index.csv";
constexpr auto INDEX_META_DATA_FILE = "../../out/timestamp/index_meta_data_int_index.csv";
constexpr auto TBL_FILE = "../../data/50mio_timestamps.tbl";

// table and compression settings
///////////////////////////////
constexpr auto TABLE_NAME_PREFIX = "timestamp";
const auto CHUNK_SIZE = size_t{5'000'000};
const auto CHUNK_ENCODINGS = std::vector{SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::LZ4}, SegmentEncodingSpec{EncodingType::RunLength}};

///////////////////////////////
// methods
///////////////////////////////

std::string get_table_name(const std::string table_name, const std::string encoding) {
  return table_name + "_encoding_" + encoding;
} 

}  // namespace

///////////////////////////////
// Fixtures
///////////////////////////////

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class TimestampMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) {
    auto& storage_manager = Hyrise::get().storage_manager;

    // Generate tables
    if (!_data_generated) {

      // file for table stats
      std::ofstream segment_meta_data_csv_file(SEGMENT_META_DATA_FILE);
      segment_meta_data_csv_file << "TABLE_NAME,COLUMN_ID,ENCODING,CHUNK_ID,ROW_COUNT,SIZE_IN_BYTES\n";

      std::ofstream index_meta_data_csv_file(INDEX_META_DATA_FILE);
      index_meta_data_csv_file << "TABLE_NAME,COLUMN_ID,ENCODING,CHUNK_ID,ROW_COUNT,SIZE_IN_BYTES\n"; 

      for (const opossum::SegmentEncodingSpec & encoding : CHUNK_ENCODINGS) {
        const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
        const auto new_table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);

        auto new_table = load_table(TBL_FILE, CHUNK_SIZE);
        auto table_wrapper = std::make_shared<TableWrapper>(new_table);
        table_wrapper->execute();
        const auto chunk_encoding_spec = ChunkEncodingSpec(table_wrapper->get_output()->column_count(), {encoding});
        
        ChunkEncoder::encode_all_chunks(new_table, chunk_encoding_spec);

        storage_manager.add_table(new_table_name, new_table);
        std::cout << "Created table: " << new_table_name << std::endl;

        for (auto column_id = ColumnID{0}; column_id < new_table->column_count(); ++column_id) {
          for (auto chunk_id = ChunkID{0}, end = new_table->chunk_count(); chunk_id < end;  ++chunk_id) {
            const auto& chunk = new_table->get_chunk(chunk_id);
            const auto& segment = chunk->get_segment(column_id);
            segment_meta_data_csv_file << new_table_name << "," << new_table->column_name(column_id) << "," << encoding << "," << chunk_id << "," << CHUNK_SIZE << "," << segment->memory_usage(MemoryUsageCalculationMode::Full) << "\n";
          }
        }
      }
      segment_meta_data_csv_file.close();
      index_meta_data_csv_file.close();
    }
    _data_generated = true;
  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State&) {}

  inline static bool _data_generated = false;

};

///////////////////////////////
// benchmarks
///////////////////////////////

static void BM_TestTimestamp_Print_Tables(benchmark::State& state) {
  // debug print of unsorted table
  constexpr auto chunk_size = size_t{10'000'000};
  auto table = load_table("../../data/small_timestamps.tbl", chunk_size);
  Print::print(table);
}
BENCHMARK(BM_TestTimestamp_Print_Tables);


/// Strings 
///////////

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_String_Q1)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("STRING");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "STRING");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_("2018-11-01 23:58:46"));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
    //std::cout << "#####" << std::endl;
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_String_Q2)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("STRING");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "STRING");

  auto predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_("2018-11-01 21:01:44"), value_("2018-11-08 02:35:23"));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_String_Q3)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("STRING");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "STRING");

  auto predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_("2018-11-01 21:01:00"), value_("2018-11-01 21:03:59"));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_String_Q4)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("STRING");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "STRING");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Like, operand, value_("2018-11-02%"));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}


BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_String_Q5)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("STRING");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "STRING");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Like, operand, value_("% 12:%"));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

/// Unix
////////

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Unix_Q1)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("UNIX");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "UNIX");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(1541116726));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Unix_Q2)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("UNIX");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "UNIX");

  auto predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541106104), value_(1541644523));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Unix_Q3)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("UNIX");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "UNIX");

  auto predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541106060), value_(1541106239));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Unix_Q4)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("UNIX");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "UNIX");

  auto predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541116800), value_(1541203199));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Unix_Q5)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("UNIX");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "UNIX");

  auto predicate0 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541073600), value_(1541077199));
  auto predicate1 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541160000), value_(1541163599));
  auto predicate2 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541246400), value_(1541249999));
  auto predicate3 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541332800), value_(1541336399));
  auto predicate4 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541419200), value_(1541422799));
  auto predicate5 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541505600), value_(1541509199));
  auto predicate6 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541592000), value_(1541595599));
  auto predicate7 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541678400), value_(1541681999));
  auto predicate8 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541764800), value_(1541768399));
  auto predicate9 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541851200), value_(1541854799));
  auto predicate10 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1541937600), value_(1541941199));
  auto predicate11 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_(1542024000), value_(1542027599));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan0 = std::make_shared<TableScan>(table_wrapper, predicate0);
  warm_up_table_scan0->execute();
  const auto warm_up_table_scan1 = std::make_shared<TableScan>(table_wrapper, predicate1);
  warm_up_table_scan1->execute();
  const auto warm_up_table_scan2 = std::make_shared<TableScan>(table_wrapper, predicate2);
  warm_up_table_scan2->execute();
  const auto warm_up_table_scan3 = std::make_shared<TableScan>(table_wrapper, predicate3);
  warm_up_table_scan3->execute();
  const auto warm_up_table_scan4 = std::make_shared<TableScan>(table_wrapper, predicate4);
  warm_up_table_scan4->execute();
  const auto warm_up_table_scan5 = std::make_shared<TableScan>(table_wrapper, predicate5);
  warm_up_table_scan5->execute();
  const auto warm_up_table_scan6 = std::make_shared<TableScan>(table_wrapper, predicate6);
  warm_up_table_scan6->execute();
  const auto warm_up_table_scan7 = std::make_shared<TableScan>(table_wrapper, predicate7);
  warm_up_table_scan7->execute();
  const auto warm_up_table_scan8 = std::make_shared<TableScan>(table_wrapper, predicate8);
  warm_up_table_scan8->execute();
  const auto warm_up_table_scan9 = std::make_shared<TableScan>(table_wrapper, predicate9);
  warm_up_table_scan9->execute();
  const auto warm_up_table_scan10 = std::make_shared<TableScan>(table_wrapper, predicate10);
  warm_up_table_scan10->execute();
  const auto warm_up_table_scan11 = std::make_shared<TableScan>(table_wrapper, predicate11);
  warm_up_table_scan11->execute();

  for (auto _ : state) {
    const auto table_scan0 = std::make_shared<TableScan>(table_wrapper, predicate0);
    const auto table_scan1 = std::make_shared<TableScan>(table_wrapper, predicate1);
    const auto table_scan2 = std::make_shared<TableScan>(table_wrapper, predicate2);
    const auto table_scan3 = std::make_shared<TableScan>(table_wrapper, predicate3);
    const auto table_scan4 = std::make_shared<TableScan>(table_wrapper, predicate4);
    const auto table_scan5 = std::make_shared<TableScan>(table_wrapper, predicate5);
    const auto table_scan6 = std::make_shared<TableScan>(table_wrapper, predicate6);
    const auto table_scan7 = std::make_shared<TableScan>(table_wrapper, predicate7);
    const auto table_scan8 = std::make_shared<TableScan>(table_wrapper, predicate8);
    const auto table_scan9 = std::make_shared<TableScan>(table_wrapper, predicate9);
    const auto table_scan10 = std::make_shared<TableScan>(table_wrapper, predicate10);
    const auto table_scan11 = std::make_shared<TableScan>(table_wrapper, predicate11);
    
    table_scan0->execute();
    table_scan1->execute();
    table_scan2->execute();
    table_scan3->execute();
    table_scan4->execute();
    table_scan5->execute();
    table_scan6->execute();
    table_scan7->execute();
    table_scan8->execute();
    table_scan9->execute();
    table_scan10->execute();
    table_scan11->execute();
    
    auto union0 = std::make_shared<UnionAll>(table_scan0, table_scan1);
    union0->execute();
    auto union1 = std::make_shared<UnionAll>(union0, table_scan2);
    union1->execute();
    auto union2 = std::make_shared<UnionAll>(union1, table_scan3);
    union2->execute();
    auto union3 = std::make_shared<UnionAll>(union2, table_scan4);
    union3->execute();
    auto union4 = std::make_shared<UnionAll>(union3, table_scan5);
    union4->execute();
    auto union5 = std::make_shared<UnionAll>(union4, table_scan6);
    union5->execute();
    auto union6 = std::make_shared<UnionAll>(union5, table_scan7);
    union6->execute();
    auto union7 = std::make_shared<UnionAll>(union6, table_scan8);
    union7->execute();
    auto union8 = std::make_shared<UnionAll>(union7, table_scan9);
    union8->execute();
    auto union9 = std::make_shared<UnionAll>(union8, table_scan10);
    union9->execute();
    auto union10 = std::make_shared<UnionAll>(union9, table_scan11);
    union10->execute();

    //std::cout << *table_scan << std::endl;
    //auto r = union_all->get_output();
    //Print::print(r);
  }
}

/// DateTime 
///////////

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_DateTime_Q1)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("TIME");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "TIME");

  const auto scan_column_id1 = table->column_id_by_name("DATE");
  auto operand1 = pqp_column_(scan_column_id1, table->column_data_type(scan_column_id1), false, "DATE");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_("23:58:46"));
  auto predicate1 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand1, value_("2018-11-01"));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();
  const auto warm_up_table_scan1 = std::make_shared<TableScan>(table_wrapper, predicate1);
  warm_up_table_scan1->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    const auto table_scan1 = std::make_shared<TableScan>(table_scan, predicate1);
    table_scan1->execute();

    //std::cout << *table_scan << std::endl;
    //auto r = table_scan1->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_DateTime_Q2)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("TIME");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "TIME");

  const auto scan_column_id1 = table->column_id_by_name("DATE");
  auto operand1 = pqp_column_(scan_column_id1, table->column_data_type(scan_column_id1), false, "DATE");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, operand, value_("21:01:44"));
  auto predicate1 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThanEquals, operand1, value_("2018-11-01"));
  auto predicate2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThan, operand, value_("02:35:23"));
  auto predicate3 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand1, value_("2018-11-08"));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();
  const auto warm_up_table_scan1 = std::make_shared<TableScan>(table_wrapper, predicate1);
  warm_up_table_scan1->execute();
  const auto warm_up_table_scan2 = std::make_shared<TableScan>(table_wrapper, predicate2);
  warm_up_table_scan2->execute();
  const auto warm_up_table_scan3 = std::make_shared<TableScan>(table_wrapper, predicate3);
  warm_up_table_scan3->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    const auto table_scan1 = std::make_shared<TableScan>(table_scan, predicate1);
    table_scan1->execute();

    const auto table_scan2 = std::make_shared<TableScan>(table_wrapper, predicate2);
    table_scan2->execute();
    const auto table_scan3 = std::make_shared<TableScan>(table_scan2, predicate3);
    table_scan3->execute();

    auto union_all = std::make_shared<UnionAll>(table_scan1, table_scan3);
    union_all->execute();

    //std::cout << *table_scan << std::endl;
    //auto r = table_scan1->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_DateTime_Q3)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("TIME");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "TIME");

  const auto scan_column_id1 = table->column_id_by_name("DATE");
  auto operand1 = pqp_column_(scan_column_id1, table->column_data_type(scan_column_id1), false, "DATE");

  auto predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand, value_("21:01:00"), value_("21:03:59"));
  auto predicate1 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand1, value_("2018-11-01"), value_("2018-11-01"));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();
  const auto warm_up_table_scan1 = std::make_shared<TableScan>(table_wrapper, predicate1);
  warm_up_table_scan1->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    const auto table_scan1 = std::make_shared<TableScan>(table_scan, predicate1);
    table_scan1->execute();

    //std::cout << *table_scan << std::endl;
    //auto r = table_scan1->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_DateTime_Q4)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("DATE");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "DATE");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_("2018-11-02"));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_DateTime_Q5)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("TIME");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "TIME");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Like, operand, value_("12:%"));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// Split
////////////////////////////////////////////////////////////////////////////////////////////////////

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Split_Q1)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("SECOND");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "SECOND");

  const auto scan_column_id1 = table->column_id_by_name("MINUTE");
  auto operand1 = pqp_column_(scan_column_id1, table->column_data_type(scan_column_id1), false, "MINUTE");

  const auto scan_column_id2 = table->column_id_by_name("HOUR");
  auto operand2 = pqp_column_(scan_column_id2, table->column_data_type(scan_column_id2), false, "HOUR");

  const auto scan_column_id3 = table->column_id_by_name("DAY");
  auto operand3 = pqp_column_(scan_column_id3, table->column_data_type(scan_column_id3), false, "DAY");

  const auto scan_column_id4 = table->column_id_by_name("MONTH");
  auto operand4 = pqp_column_(scan_column_id4, table->column_data_type(scan_column_id4), false, "MONTH");

  const auto scan_column_id5 = table->column_id_by_name("YEAR");
  auto operand5 = pqp_column_(scan_column_id5, table->column_data_type(scan_column_id5), false, "YEAR");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(46));
  auto predicate1 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand1, value_(58));
  auto predicate2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand2, value_(23));
  auto predicate3 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand3, value_(1));
  auto predicate4 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand4, value_(11));
  auto predicate5 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand5, value_(2018));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();
  const auto warm_up_table_scan1 = std::make_shared<TableScan>(table_wrapper, predicate1);
  warm_up_table_scan1->execute();
  const auto warm_up_table_scan2 = std::make_shared<TableScan>(table_wrapper, predicate2);
  warm_up_table_scan2->execute();
  const auto warm_up_table_scan3 = std::make_shared<TableScan>(table_wrapper, predicate3);
  warm_up_table_scan3->execute();
  const auto warm_up_table_scan4 = std::make_shared<TableScan>(table_wrapper, predicate4);
  warm_up_table_scan4->execute();
  const auto warm_up_table_scan5 = std::make_shared<TableScan>(table_wrapper, predicate5);
  warm_up_table_scan5->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
    const auto table_scan1 = std::make_shared<TableScan>(table_scan, predicate1);
    table_scan1->execute();
    const auto table_scan2 = std::make_shared<TableScan>(table_scan1, predicate2);
    table_scan2->execute();
    const auto table_scan3 = std::make_shared<TableScan>(table_scan2, predicate3);
    table_scan3->execute();
    const auto table_scan4 = std::make_shared<TableScan>(table_scan3, predicate4);
    table_scan4->execute();
    const auto table_scan5 = std::make_shared<TableScan>(table_scan4, predicate5);
    table_scan5->execute();

    //std::cout << *table_scan << std::endl;
    //auto r = table_scan5->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Split_Q2)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("SECOND");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "SECOND");

  const auto scan_column_id1 = table->column_id_by_name("MINUTE");
  auto operand1 = pqp_column_(scan_column_id1, table->column_data_type(scan_column_id1), false, "MINUTE");

  const auto scan_column_id2 = table->column_id_by_name("HOUR");
  auto operand2 = pqp_column_(scan_column_id2, table->column_data_type(scan_column_id2), false, "HOUR");

  const auto scan_column_id3 = table->column_id_by_name("DAY");
  auto operand3 = pqp_column_(scan_column_id3, table->column_data_type(scan_column_id3), false, "DAY");

  const auto scan_column_id4 = table->column_id_by_name("MONTH");
  auto operand4 = pqp_column_(scan_column_id4, table->column_data_type(scan_column_id4), false, "MONTH");

  const auto scan_column_id5 = table->column_id_by_name("YEAR");
  auto operand5 = pqp_column_(scan_column_id5, table->column_data_type(scan_column_id5), false, "YEAR");

  auto min_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand5, value_(2018));
  auto min_predicate1 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand4, value_(11));
  auto min_predicate2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThanEquals, operand3, value_(1));
  auto min_predicate3 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThanEquals, operand2, value_(21));
  auto min_predicate4 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThanEquals, operand1, value_(1));
  auto min_predicate5 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, operand, value_(44));

  auto max_predicate2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand3, value_(8));
  auto max_predicate3 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand2, value_(2));
  auto max_predicate4 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand1, value_(35));
  auto max_predicate5 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThan, operand, value_(23));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, min_predicate);
  warm_up_table_scan->execute();
  const auto warm_up_table_scan1 = std::make_shared<TableScan>(table_wrapper, min_predicate1);
  warm_up_table_scan1->execute();
  const auto warm_up_table_scan2 = std::make_shared<TableScan>(table_wrapper, min_predicate2);
  warm_up_table_scan2->execute();
  const auto warm_up_table_scan3 = std::make_shared<TableScan>(table_wrapper, min_predicate3);
  warm_up_table_scan3->execute();
  const auto warm_up_table_scan4 = std::make_shared<TableScan>(table_wrapper, min_predicate4);
  warm_up_table_scan4->execute();
  const auto warm_up_table_scan5 = std::make_shared<TableScan>(table_wrapper, min_predicate5);
  warm_up_table_scan5->execute();

  const auto warm_up_table_scan8 = std::make_shared<TableScan>(table_wrapper, max_predicate2);
  warm_up_table_scan8->execute();
  const auto warm_up_table_scan9 = std::make_shared<TableScan>(table_wrapper, max_predicate3);
  warm_up_table_scan9->execute();
  const auto warm_up_table_scan10 = std::make_shared<TableScan>(table_wrapper, max_predicate4);
  warm_up_table_scan10->execute();
  const auto warm_up_table_scan11 = std::make_shared<TableScan>(table_wrapper, max_predicate5);
  warm_up_table_scan11->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, min_predicate);
    table_scan->execute();
    const auto table_scan1 = std::make_shared<TableScan>(table_scan, min_predicate1);
    table_scan1->execute();
    const auto table_scan2 = std::make_shared<TableScan>(table_scan1, min_predicate2);
    table_scan2->execute();
    const auto table_scan3 = std::make_shared<TableScan>(table_scan2, min_predicate3);
    table_scan3->execute();
    const auto table_scan4 = std::make_shared<TableScan>(table_scan3, min_predicate4);
    table_scan4->execute();
    const auto table_scan5 = std::make_shared<TableScan>(table_scan4, min_predicate5);
    table_scan5->execute();


    const auto table_scan8 = std::make_shared<TableScan>(table_scan1, max_predicate2);
    table_scan8->execute();
    const auto table_scan9 = std::make_shared<TableScan>(table_scan8, max_predicate3);
    table_scan9->execute();
    const auto table_scan10 = std::make_shared<TableScan>(table_scan9, max_predicate4);
    table_scan10->execute();
    const auto table_scan11 = std::make_shared<TableScan>(table_scan10, max_predicate5);
    table_scan11->execute();

    auto union_all = std::make_shared<UnionAll>(table_scan11, table_scan5);
    union_all->execute();

    //std::cout << *table_scan << std::endl;
    //auto r = table_scan11->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Split_Q3)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id1 = table->column_id_by_name("MINUTE");
  auto operand1 = pqp_column_(scan_column_id1, table->column_data_type(scan_column_id1), false, "MINUTE");

  const auto scan_column_id2 = table->column_id_by_name("HOUR");
  auto operand2 = pqp_column_(scan_column_id2, table->column_data_type(scan_column_id2), false, "HOUR");

  const auto scan_column_id3 = table->column_id_by_name("DAY");
  auto operand3 = pqp_column_(scan_column_id3, table->column_data_type(scan_column_id3), false, "DAY");

  const auto scan_column_id4 = table->column_id_by_name("MONTH");
  auto operand4 = pqp_column_(scan_column_id4, table->column_data_type(scan_column_id4), false, "MONTH");

  const auto scan_column_id5 = table->column_id_by_name("YEAR");
  auto operand5 = pqp_column_(scan_column_id5, table->column_data_type(scan_column_id5), false, "YEAR");

  auto min_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand5, value_(2018));
  auto min_predicate1 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand4, value_(11));
  auto min_predicate2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand3, value_(1));
  auto min_predicate3 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand2, value_(21));
  auto min_predicate4 = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, operand1, value_(1), value_(3));

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, min_predicate);
  warm_up_table_scan->execute();
  const auto warm_up_table_scan1 = std::make_shared<TableScan>(table_wrapper, min_predicate1);
  warm_up_table_scan1->execute();
  const auto warm_up_table_scan2 = std::make_shared<TableScan>(table_wrapper, min_predicate2);
  warm_up_table_scan2->execute();
  const auto warm_up_table_scan3 = std::make_shared<TableScan>(table_wrapper, min_predicate3);
  warm_up_table_scan3->execute();
  const auto warm_up_table_scan4 = std::make_shared<TableScan>(table_wrapper, min_predicate4);
  warm_up_table_scan4->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, min_predicate3);
    table_scan->execute();
    const auto table_scan1 = std::make_shared<TableScan>(table_scan, min_predicate1);
    table_scan1->execute();
    const auto table_scan2 = std::make_shared<TableScan>(table_scan1, min_predicate2);
    table_scan2->execute();
    const auto table_scan3 = std::make_shared<TableScan>(table_scan2, min_predicate);
    table_scan3->execute();
    const auto table_scan4 = std::make_shared<TableScan>(table_scan3, min_predicate4);
    table_scan4->execute();

    //std::cout << *table_scan << std::endl;
    //auto r = table_scan4->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Split_Q4)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("DAY");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "DAY");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(2));
  
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();

    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

BENCHMARK_DEFINE_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Split_Q5)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto encoding = CHUNK_ENCODINGS[state.range(0)];
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name("HOUR");
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, "HOUR");

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(12));
  
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();

    //std::cout << *table_scan << std::endl;
    //auto r = table_scan->get_output();
    //Print::print(r);
  }
}

static void CustomArguments(benchmark::internal::Benchmark* b) {
  for (size_t encoding_id = 0; encoding_id < CHUNK_ENCODINGS.size(); ++encoding_id) {
    b->Args({static_cast<long long>(encoding_id)});
  }
}

// STRING Benchmarks 
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_String_Q1)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_String_Q2)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_String_Q3)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_String_Q4)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_String_Q5)->Apply(CustomArguments);

// UNIX Benchmarks
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Unix_Q1)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Unix_Q2)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Unix_Q3)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Unix_Q4)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Unix_Q5)->Apply(CustomArguments);


// Date Time Benchmarks
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_DateTime_Q1)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_DateTime_Q2)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_DateTime_Q3)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_DateTime_Q4)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_DateTime_Q5)->Apply(CustomArguments);

// Split Benchmarks
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Split_Q1)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Split_Q2)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Split_Q3)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Split_Q4)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(TimestampMicroBenchmarkFixture, BM_Timestamp_Split_Q5)->Apply(CustomArguments);

}  // namespace opossum
