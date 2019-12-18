#include <fstream>

#include "micro_benchmark_basic_fixture.hpp"

#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/index_scan.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
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
constexpr auto TABLE_NAME_PREFIX = "ping";
//constexpr auto TBL_FILE = "../../data/10mio_pings_int.tbl";
//constexpr auto TBL_FILE = "../../data/10mio_pings.tbl";
constexpr auto TBL_FILE = "../../data/100_pings.tbl";
const auto CHUNK_SIZES = std::vector{size_t{1'000'000}};
//const auto CHUNK_SIZES = std::vector{size_t{10}};
const auto ORDER_COLUMNS = std::vector{"captain_id", "latitude", "longitude", "timestamp", "captain_status"};
// TODO: evaluate Frame of Reference as well, fall back to dictionary for unsupported data types
//const auto CHUNK_ENCODINGS = std::vector{EncodingType::Unencoded, EncodingType::Dictionary, EncodingType::LZ4, EncodingType::RunLength, EncodingType::FrameOfReference};
const auto CHUNK_ENCODINGS = std::vector{EncodingType::Unencoded, EncodingType::Dictionary, EncodingType::LZ4, EncodingType::RunLength};

// single value benchmark values (median, min, max)
// determined by column stats python script
//const auto BM_VAL_CAPTAIN_ID = std::vector{298186, 59547, 1211286};
//const auto BM_VAL_CAPTAIN_STATUS = std::vector{2, 1, 2};
//const auto BM_VAL_LATITUDE = std::vector{25.1388267, 24.9154559, 25.1916198};
//const auto BM_VAL_TIMESTAMP = std::vector{"2018-12-06 23:57:34", "2018-12-22 23:22:11", "2018-11-05 05:38:15"};
//const auto BM_SCAN_VALUES = BM_VAL_CAPTAIN_ID.size();

// quantile benchmark values
// determined by column stats python script calculated with pandas, settings nearest 
const auto BM_VAL_CAPTAIN_ID = std::vector{4, 4115, 11787, 57069, 176022, 451746, 616628, 901080, 1156169, 1233112, 1414788};
const auto BM_VAL_CAPTAIN_STATUS = std::vector{1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2};
const auto BM_VAL_LATITUDE = std::vector{10.2191832, 25.0455204, 25.0699667, 25.0872227, 25.1030861, 25.1244186, 25.1724729, 25.1966912, 25.2164364, 25.2437205, 60.1671321};
const auto BM_VAL_LONGITUDE = std::vector{-213.5243575, 55.1423584, 55.1549474, 55.1792718, 55.2072508, 55.2470842, 55.2692599, 55.2806365, 55.3156638, 55.3640991, 212.33914};

//timestamp values string and unix timestamp
//const auto BM_VAL_TIMESTAMP = std::vector{1541372479, 1541398637, 1541418563, 1541439501, 1542924244, 1545434420, 1545474895, 1545518017, 1548644767, 1548671748, 1548716462};
const auto BM_VAL_TIMESTAMP = std::vector{"2018-11-05 00:01:19", "2018-11-05 07:17:17", "2018-11-05 12:49:23", "2018-11-05 18:38:21", "2018-11-22 23:04:04", "2018-12-22 00:20:20", "2018-12-22 11:34:55", "2018-12-22 23:33:37", "2019-01-28 04:06:07", "2019-01-28 11:35:48", "2019-01-29 00:01:02"};
const auto BM_SCAN_VALUES = BM_VAL_CAPTAIN_ID.size();

///////////////////////////////
// methods
///////////////////////////////

Segments get_segments_of_chunk(const std::shared_ptr<const Table>& input_table, ChunkID chunk_id){
  Segments segments{};
  for (auto column_id = ColumnID{0}; column_id < input_table->column_count(); ++column_id) {
    segments.emplace_back(input_table->get_chunk(chunk_id)->get_segment(column_id));
  }
  return segments;
} 

std::shared_ptr<Table> sort_table_chunk_wise(const std::shared_ptr<const Table>& input_table,
    const std::string order_by_column_name, const size_t chunk_size, const std::optional<ChunkEncodingSpec>& chunk_encoding_spec = std::nullopt,
    const OrderByMode order_by_mode = OrderByMode::Ascending) {
  // empty table to which we iteratively add the sorted chunks
  auto sorted_table = std::make_shared<Table>(input_table->column_definitions(), TableType::Data, chunk_size, UseMvcc::No);

  const auto chunk_count = input_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    // create new single chunk and create a new table with that chunk
    auto new_chunk = std::make_shared<Chunk>(get_segments_of_chunk(input_table, chunk_id));
    std::vector<std::shared_ptr<Chunk>> single_chunk_to_sort_as_vector = {new_chunk};
    auto single_chunk_table = std::make_shared<Table>(input_table->column_definitions(), TableType::Data, std::move(single_chunk_to_sort_as_vector), UseMvcc::No);

    // call sort operator on single-chunk table
    auto table_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
    table_wrapper->execute();
    auto sort = std::make_shared<Sort>(table_wrapper, single_chunk_table->column_id_by_name(order_by_column_name), order_by_mode, chunk_size);
    sort->execute();
    const auto immutable_sorted_table = sort->get_output();

    // add sorted chunk to output table
    // Note: we do not care about MVCC at all at the moment
    sorted_table->append_chunk(get_segments_of_chunk(immutable_sorted_table, ChunkID{0}));
    //sorted_table->append_chunk(immutable_sorted_table->get_chunk(ChunkID{0})->segments());
    const auto& added_chunk = sorted_table->get_chunk(chunk_id);
    added_chunk->set_ordered_by(std::make_pair(sorted_table->column_id_by_name(order_by_column_name), order_by_mode));
    added_chunk->finalize();

    // in case a chunk encoding spec is provided, encode chunk
    if (chunk_encoding_spec) {
      ChunkEncoder::encode_chunk(added_chunk, immutable_sorted_table->column_data_types(), *chunk_encoding_spec);
    }
  }

  return sorted_table;
}

std::string get_table_name(const std::string table_name, const std::size_t chunk_size, const std::string order_by_column , const std::string encoding) {
  return table_name + "_csize_" + std::to_string(chunk_size) + "_orderby_" + order_by_column + "_encoding_" + encoding;
} 

}  // namespace




///////////////////////////////
// Fixtures
///////////////////////////////

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class PingDataMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) {
    auto& storage_manager = Hyrise::get().storage_manager;

    // Generate tables
    if (!_data_generated) {

      // file for table stats
      std::ofstream segment_meta_data_csv_file("../../out/segment_meta_data.csv");
      segment_meta_data_csv_file << "TABLE_NAME,COLUMN_ID,ORDER_BY,ENCODING,CHUNK_ID,CHUNK_SIZE,ROW_COUNT,SIZE_IN_BYTES\n";
      
      // Sort table and add sorted tables to the storage manager
      for (const auto chunk_size : CHUNK_SIZES) {
        // Load origninal table from tbl file with specified chunk size
        std::cout << "Load initial table form tbl file '" << TBL_FILE << "' with chunk size: " << chunk_size << "." << std::endl;
        auto loaded_table = load_table(TBL_FILE, chunk_size);

        for (const auto order_by_column : ORDER_COLUMNS) {
          for (const auto encoding : CHUNK_ENCODINGS) {

            // Maybe there is a better way to do this
            const auto encoding_type = encoding_type_to_string.left.at(encoding);
            const auto sorted_table_name = get_table_name(TABLE_NAME_PREFIX, chunk_size, order_by_column, encoding_type);
            auto table_wrapper = std::make_shared<TableWrapper>(loaded_table);
            table_wrapper->execute();
            const auto chunk_encoding_spec = ChunkEncodingSpec(table_wrapper->get_output()->column_count(), {SegmentEncodingSpec{encoding}});

            auto sorted_table = sort_table_chunk_wise(loaded_table, order_by_column, chunk_size, chunk_encoding_spec);
            storage_manager.add_table(sorted_table_name, sorted_table);
            std::cout << "Created table: " << sorted_table_name << std::endl;

            // table stats
            for (auto column_id = ColumnID{0}; column_id < sorted_table->column_count(); ++column_id) {
              for (auto chunk_id = ChunkID{0}, end = sorted_table->chunk_count(); chunk_id < end;  ++chunk_id) {
                const auto& chunk = sorted_table->get_chunk(chunk_id);
                const auto& segment = chunk->get_segment(column_id);

                segment_meta_data_csv_file << sorted_table_name << "," << sorted_table->column_name(column_id) << ","<< order_by_column << ","<< encoding << ","<< chunk_id << "," << chunk_size << "," << segment->size() << "," << segment->estimate_memory_usage() << "\n";
              }
            }

            // create index 
            if (order_by_column == ORDER_COLUMNS[0] && encoding == EncodingType::Dictionary) {
              const auto column_count = sorted_table->column_count();
              std::cout << "Creating indexes: ";
              for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
                sorted_table->create_index<GroupKeyIndex>({column_id});
              }
              std::cout << " done." << std::endl;
            }
          }
        }
      }

      segment_meta_data_csv_file.close();

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

static void BM_Ping_Print_Sorted_Tables(benchmark::State& state) {
  // debug print of unsorted table
  constexpr auto chunk_size = size_t{10};
  auto table = load_table("../../data/100_pings.tbl", chunk_size);
  std::cout << "############ unsorted:" << std::endl;
  Print::print(table);

  auto sorted_table = sort_table_chunk_wise(table, "timestamp", chunk_size);

  // debug print of unsorted table
  std::cout << "############ sorted:" << std::endl;
  Print::print(sorted_table);

  for (auto _ : state) {
    auto sorted_table_benchmark = sort_table_chunk_wise(table, "timestamp", chunk_size);
  }
}
BENCHMARK(BM_Ping_Print_Sorted_Tables);

// investigate which scan operation is used (more a test than a benchmark)
BENCHMARK_DEFINE_F(PingDataMicroBenchmarkFixture, BM_Penis_Test)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto chunk_size = CHUNK_SIZES[state.range(0)];
  const auto order_by_column = ORDER_COLUMNS[state.range(1)];
  const auto encoding = CHUNK_ENCODINGS[state.range(2)];
  const auto sort_column = ORDER_COLUMNS[state.range(3)];


  const auto encoding_type = encoding_type_to_string.left.at(encoding);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, chunk_size, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  auto operand = pqp_column_(table->column_id_by_name(sort_column), table->column_data_type(table->column_id_by_name(sort_column)), false, sort_column);

  // scan
  std::shared_ptr<BinaryPredicateExpression> predicate;
  // should by nicer dicer
  if (state.range(3) == 0) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_CAPTAIN_ID[state.range(4)]));}
  if (state.range(3) == 1) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_LATITUDE[state.range(4)]));}
  if (state.range(3) == 2) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_LONGITUDE[state.range(4)]));}
  if (state.range(3) == 3) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_TIMESTAMP[state.range(4)]));}
  if (state.range(3) == 4) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_CAPTAIN_STATUS[state.range(4)]));}

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  
  const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  table_scan->execute();
  std::cout << "Test:" << state.range(0) <<  state.range(1) << state.range(2) << state.range(3) << state.range(4) <<std::endl;
}

BENCHMARK_DEFINE_F(PingDataMicroBenchmarkFixture, BM_Keven_OrderingGreaterThanEqualsPerformance)(benchmark::State& state) {
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_CAPTAIN_STATUS.size(), "Sample search values for columns should have the same length.");
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_LATITUDE.size(), "Sample search values for columns should have the same length.");
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_TIMESTAMP.size(), "Sample search values for columns should have the same length.");
  Assert(BM_VAL_TIMESTAMP.size() == BM_SCAN_VALUES, "Sample search values for columns should have the same length.");

  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto chunk_size = CHUNK_SIZES[state.range(0)];
  const auto order_by_column = ORDER_COLUMNS[state.range(1)];
  const auto encoding = CHUNK_ENCODINGS[state.range(2)];
  const auto scan_column_index = state.range(3);
  const auto scan_column = ORDER_COLUMNS[scan_column_index];
  const auto search_value_index = state.range(4);


  const auto encoding_type = encoding_type_to_string.left.at(encoding);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, chunk_size, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name(scan_column);
  const auto order_by_column_id = table->column_id_by_name(order_by_column);
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, scan_column);

  // scan
  std::shared_ptr<BinaryPredicateExpression> predicate;
  // should by nicer dicer
  if (scan_column_index == 0) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand, value_(BM_VAL_CAPTAIN_ID[search_value_index]));}
  if (scan_column_index == 1) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand, value_(BM_VAL_LATITUDE[search_value_index]));}
  if (scan_column_index == 2) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand, value_(BM_VAL_LONGITUDE[search_value_index]));}
  if (scan_column_index == 3) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand, value_(BM_VAL_TIMESTAMP[search_value_index]));}
  if (scan_column_index == 4) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand, value_(BM_VAL_CAPTAIN_STATUS[search_value_index]));}

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  ////
  //// Assertions for the correction scan method, encoding types, and expected sort orders.
  ////
  if (warm_up_table_scan->get_output()->row_count() == 0) {
    std::cout << "Warning: executed table scan did not yield any results." << std::endl;
  }

  // if (order_by_column == scan_column) {
  //   const auto description = warm_up_table_scan->description(DescriptionMode::SingleLine);
  //   Assert(description.find("scanned with binary search]") != std::string::npos, "Requested a sorted scan on colum " + scan_column + ", but impl description is:\n\t" + description);
  //   Assert(description.find("ColumnVsValue") != std::string::npos, "Executed scan on colum " + scan_column + " was no ColumnVsValue scan.");
  // }
  
  const auto chunk_count = table->chunk_count();
  const auto column_count = table->column_count();
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    resolve_data_type(table->column_data_type(column_id), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto segment = table->get_chunk(chunk_id)->get_segment(column_id);

          const auto unencoded_segment = std::dynamic_pointer_cast<const ValueSegment<ColumnDataType>>(segment);
          if (unencoded_segment) {
            Assert(encoding == EncodingType::Unencoded, "Encoding type >>Unencoded<< requested for " + scan_column + " but not found.");
          } else {
            const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
            Assert(encoded_segment && encoded_segment->encoding_type() == encoding, "Encoding type not as requested for " + scan_column + ".");
          }

          if (column_id == order_by_column_id) {
            Assert(table->get_chunk(chunk_id)->ordered_by()->first == ColumnID{column_id}, "Chunk is unsorted, but it should be.");
          } else {
            Assert(table->get_chunk(chunk_id)->ordered_by() == std::nullopt || table->get_chunk(chunk_id)->ordered_by()->first != ColumnID{column_id}, "Chunk shall not be sorted, but is.");
          }
      }
    });
  }
  ////
  //// Assertions End
  ////
  
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(PingDataMicroBenchmarkFixture, BM_Keven_OrderingEqualsPerformance)(benchmark::State& state) {
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_CAPTAIN_STATUS.size(), "Sample search values for columns should have the same length.");
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_LATITUDE.size(), "Sample search values for columns should have the same length.");
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_TIMESTAMP.size(), "Sample search values for columns should have the same length.");
  Assert(BM_VAL_TIMESTAMP.size() == BM_SCAN_VALUES, "Sample search values for columns should have the same length.");

  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto chunk_size = CHUNK_SIZES[state.range(0)];
  const auto order_by_column = ORDER_COLUMNS[state.range(1)];
  const auto encoding = CHUNK_ENCODINGS[state.range(2)];
  const auto scan_column_index = state.range(3);
  const auto scan_column = ORDER_COLUMNS[scan_column_index];
  const auto search_value_index = state.range(4);


  const auto encoding_type = encoding_type_to_string.left.at(encoding);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, chunk_size, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name(scan_column);
  const auto order_by_column_id = table->column_id_by_name(order_by_column);
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, scan_column);

  // scan
  std::shared_ptr<BinaryPredicateExpression> predicate;
  // should by nicer dicer
  if (scan_column_index == 0) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_CAPTAIN_ID[search_value_index]));}
  if (scan_column_index == 1) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_LATITUDE[search_value_index]));}
  if (scan_column_index == 2) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_LONGITUDE[search_value_index]));}
  if (scan_column_index == 3) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_TIMESTAMP[search_value_index]));}
  if (scan_column_index == 4) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_CAPTAIN_STATUS[search_value_index]));}

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  ////
  //// Assertions for non-empty results, the correction scan method, encoding types, and expected sort orders.
  ////
  if (warm_up_table_scan->get_output()->row_count() == 0) {
    std::cout << "Warning: executed table scan did not yield any results." << std::endl;
  }

  if (order_by_column == scan_column) {
   const auto description = warm_up_table_scan->description(DescriptionMode::SingleLine);
   Assert(description.find("scanned with binary search]") != std::string::npos, "Requested a sorted scan on colum " + scan_column + ", but impl description is:\n\t" + description);
   Assert(description.find("ColumnVsValue") != std::string::npos, "Executed scan on colum " + scan_column + " was no ColumnVsValue scan.");
  }
  
  const auto chunk_count = table->chunk_count();
  const auto column_count = table->column_count();
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    resolve_data_type(table->column_data_type(column_id), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto segment = table->get_chunk(chunk_id)->get_segment(column_id);

          const auto unencoded_segment = std::dynamic_pointer_cast<const ValueSegment<ColumnDataType>>(segment);
          if (unencoded_segment) {
            Assert(encoding == EncodingType::Unencoded, "Encoding type >>Unencoded<< requested for " + scan_column + " but not found.");
          } else {
            const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
            Assert(encoded_segment && encoded_segment->encoding_type() == encoding, "Encoding type not as requested for " + scan_column + ".");
          }

          if (column_id == order_by_column_id) {
            Assert(table->get_chunk(chunk_id)->ordered_by()->first == ColumnID{column_id}, "Chunk is unsorted, but it should be.");
          } else {
            Assert(table->get_chunk(chunk_id)->ordered_by() == std::nullopt || table->get_chunk(chunk_id)->ordered_by()->first != ColumnID{column_id}, "Chunk shall not be sorted, but is.");
          }
      }
    });
  }
  ////
  //// Assertions End
  ////
  
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(PingDataMicroBenchmarkFixture, BM_Keven_IndexScans)(benchmark::State& state) {
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_CAPTAIN_STATUS.size(), "Sample search values for columns should have the same length. 1");
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_LATITUDE.size(), "Sample search values for columns should have the same length. 2");
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_TIMESTAMP.size(), "Sample search values for columns should have the same length. 3");
  Assert(BM_VAL_TIMESTAMP.size() == BM_SCAN_VALUES, "Sample search values for columns should have the same length. 4");

  auto& storage_manager = Hyrise::get().storage_manager;

  const auto chunk_size = CHUNK_SIZES[state.range(0)];
  const auto order_by_column = ORDER_COLUMNS[state.range(1)];
  const auto encoding = CHUNK_ENCODINGS[state.range(2)];
  const auto scan_column_index = state.range(3);
  const auto scan_column = ORDER_COLUMNS[scan_column_index];
  const auto search_value_index = state.range(4);

  if (order_by_column != ORDER_COLUMNS[0]) state.SkipWithError("Running only for a single random sorted column (should not matter for index scans). Skipping others.");
  if (encoding != EncodingType::Dictionary) state.SkipWithError("Running only for dictionary encoding (others unsupported by the GroupKey index). Skipping others.");

  const auto encoding_type = encoding_type_to_string.left.at(encoding);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, chunk_size, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name(scan_column);
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, scan_column);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  // setting up right value (i.e., the search value)
  std::vector<AllTypeVariant> right_values;
  // should by nicer dicer
  if (scan_column_index == 0) { right_values = {BM_VAL_CAPTAIN_ID[search_value_index]}; }
  if (scan_column_index == 1) { right_values = {BM_VAL_LATITUDE[search_value_index]}; }
  if (scan_column_index == 2) { right_values = {BM_VAL_LONGITUDE[search_value_index]}; }
  if (scan_column_index == 3) { right_values = {BM_VAL_TIMESTAMP[search_value_index]}; }
  if (scan_column_index == 4) { right_values = {BM_VAL_CAPTAIN_STATUS[search_value_index]}; }

  const std::vector<ColumnID> scan_column_ids = {scan_column_id};
  for (auto _ : state) {
    const auto index_scan = std::make_shared<IndexScan>(table_wrapper, SegmentIndexType::GroupKey, scan_column_ids, PredicateCondition::GreaterThanEquals, right_values);
    index_scan->execute();
  }
}

static void CustomArguments(benchmark::internal::Benchmark* b) {
  for (size_t chunk_size_id = 0; chunk_size_id < CHUNK_SIZES.size(); ++chunk_size_id) {
    for (size_t order_by_column_id = 0; order_by_column_id < ORDER_COLUMNS.size(); ++order_by_column_id) {
      for (size_t encoding_id = 0; encoding_id < CHUNK_ENCODINGS.size(); ++encoding_id) {
        for (size_t scan_column_id = 0; scan_column_id < ORDER_COLUMNS.size(); ++scan_column_id) {
          for (size_t scan_value_id = 0; scan_value_id < BM_SCAN_VALUES; ++scan_value_id)
          {
            b->Args({static_cast<long long>(chunk_size_id), static_cast<long long>(order_by_column_id), static_cast<long long>(encoding_id), static_cast<long long>(scan_column_id), static_cast<long long>(scan_value_id)});
          }
        }
      }
    }
  }
}
BENCHMARK_REGISTER_F(PingDataMicroBenchmarkFixture, BM_Keven_OrderingGreaterThanEqualsPerformance)->Apply(CustomArguments);
//BENCHMARK_REGISTER_F(PingDataMicroBenchmarkFixture, BM_Keven_OrderingEqualsPerformance)->Apply(CustomArguments);
//BENCHMARK_REGISTER_F(PingDataMicroBenchmarkFixture, BM_Penis_Test)->Apply(CustomArguments);

//BENCHMARK_REGISTER_F(PingDataMicroBenchmarkFixture, BM_Keven_IndexScans)->Apply(CustomArguments);

}  // namespace opossum
