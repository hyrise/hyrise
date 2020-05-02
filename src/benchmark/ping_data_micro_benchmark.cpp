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

// input and output settings 
///////////////////////////////
constexpr auto SEGMENT_META_DATA_FILE = "../../out/segment_meta_data_int_index.csv";
constexpr auto INDEX_META_DATA_FILE = "../../out/index_meta_data_int_index.csv";
constexpr auto TBL_FILE = "../../data/10mio_pings_int.tbl";

// table and compression settings
///////////////////////////////
constexpr auto TABLE_NAME_PREFIX = "ping";
const auto CHUNK_SIZE = size_t{1'000'000};
const auto SCAN_COLUMNS = std::vector{"captain_id", "latitude", "longitude", "timestamp", "captain_status"};
const auto ORDER_COLUMNS = std::vector{"captain_id", "latitude", "longitude", "timestamp", "captain_status", "unsorted"};
// Frame of References supports only int columns
// Dictionary Encoding should always have the id 0
const auto CHUNK_ENCODINGS = std::vector{SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::LZ4}, SegmentEncodingSpec{EncodingType::RunLength}, SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128}};
//const auto CHUNK_ENCODINGS = std::vector{SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::LZ4}, SegmentEncodingSpec{EncodingType::RunLength}};
//const auto CHUNK_ENCODINGS = std::vector{SegmentEncodingSpec{EncodingType::Dictionary}};
const auto CREATE_INDEX = true; 

// quantile benchmark values (mixed data type table)
// determined by column stats python script calculated with pandas, settings nearest 
///////////////////////////////
// const auto BM_VAL_CAPTAIN_ID = std::vector{4, 4115, 11787, 57069, 176022, 451746, 616628, 901080, 1156169, 1233112, 1414788};
// const auto BM_VAL_CAPTAIN_STATUS = std::vector{1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2};
// const auto BM_VAL_LATITUDE = std::vector{10.2191832, 25.0455204, 25.0699667, 25.0872227, 25.1030861, 25.1244186, 25.1724729, 25.1966912, 25.2164364, 25.2437205, 60.1671321};
// const auto BM_VAL_LONGITUDE = std::vector{-213.5243575, 55.1423584, 55.1549474, 55.1792718, 55.2072508, 55.2470842, 55.2692599, 55.2806365, 55.3156638, 55.3640991, 212.33914};
// const auto BM_VAL_TIMESTAMP = std::vector{"2018-11-05 00:01:19", "2018-11-05 07:17:17", "2018-11-05 12:49:23", "2018-11-05 18:38:21", "2018-11-22 23:04:04", "2018-12-22 00:20:20", "2018-12-22 11:34:55", "2018-12-22 23:33:37", "2019-01-28 04:06:07", "2019-01-28 11:35:48", "2019-01-29 00:01:02"};

// quantile benchmark values (int table)
// timestamp values --> unix timestamp
// [0.0001, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0]
///////////////////////////////
const auto BM_VAL_CAPTAIN_ID = std::vector{4, 464, 844, 1628, 4115, 6362, 11787, 24882, 57069, 176022, 451746, 616628, 901080, 954443, 1156169, 1233112, 1414788};
const auto BM_VAL_CAPTAIN_STATUS = std::vector{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2};
const auto BM_VAL_LATITUDE = std::vector{243973175, 249279532, 249973158, 250299799, 250455204, 250573540, 250699666, 250775666, 250872227, 251030861, 251244185, 251724729, 251966912, 252081200, 252164364, 252437204, 601671321};
const auto BM_VAL_LONGITUDE = std::vector{543540652, 550593981, 551164532, 551349072, 551423584, 551481989, 551549474, 551685925, 551792718, 552072508, 552470842, 552692599, 552806365, 552898481, 553156638, 553640991, 2123391399};
const auto BM_VAL_TIMESTAMP = std::vector{1541372629, 1541379924, 1541382930, 1541389423, 1541398637, 1541408022, 1541418563, 1541428900, 1541439501, 1542924244, 1545434420, 1545474895, 1545518017, 1547639955, 1548644767, 1548671748, 1548716462};

const auto BM_SCAN_VALUES = BM_VAL_CAPTAIN_ID.size();

// quantile between benchmark values (int table)
// timestamp values --> unix timestamp
// [0.0001, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0]
///////////////////////////////
const std::vector<std::vector<int>> BM_BETWEEN_VAL_CAPTAIN_ID {{451745, 451746}, {435398, 460428}, {422254, 464467}, {403006, 496125}, {298194, 538889}, {229320, 570621}, {176022, 616628}, {140460, 695666}, {100762, 748517}, {57069, 901080}, {24882, 954443}, {11787, 1156169}, {6362, 1204047}, {4873, 1216443}, {4115, 1233112}, {1628, 1308975}, {4, 1414788}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_CAPTAIN_STATUS {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 2}, {1, 2}, {1, 2}, {1, 2}, {1, 2}, {1, 2}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_LATITUDE {{251244097, 251244248}, {251230315, 251257525}, {251209843, 251282647}, {251163169, 251357987}, {251129117, 251483861}, {251088242, 251602810}, {251030861, 251724729}, {250980490, 251843563}, {250938817, 251886562}, {250872227, 251966912}, {250775666, 252081200}, {250699666, 252164364}, {250573540, 252290339}, {250500198, 252342398}, {250455204, 252437204}, {250299799, 252579811}, {102191832, 601671321}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_LONGITUDE {{552470640, 552471070}, {552454391, 552489962}, {552431658, 552503086}, {552373107, 552528396}, {552248024, 552598718}, {552163676, 552647236}, {552072508, 552692599}, {552030164, 552730356}, {551962965, 552761497}, {551792718, 552806365}, {551685925, 552898482}, {551549474, 553156638}, {551481989, 553443178}, {551450469, 553532204}, {551423584, 553640991}, {551349072, 553979534}, {-2135243575, 2123391399}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_TIMESTAMP {{1545434360, 1545434475}, {1544130507, 1545440081}, {1544125430, 1545444223}, {1544112365, 1545451370}, {1544084375, 1545459820}, {1544070890, 1545467594}, {1542924244, 1545474895}, {1542878145, 1545483158}, {1542855912, 1545490798}, {1541439501, 1545518018}, {1541428900, 1547639955}, {1541418563, 1548644767}, {1541408022, 1548658352}, {1541403370, 1548665723}, {1541398637, 1548671748}, {1541389423, 1548687353}, {1541372479, 1548716462}};

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

    auto sort = std::make_shared<Sort>(
      table_wrapper, std::vector<SortColumnDefinition>{
        SortColumnDefinition{single_chunk_table->column_id_by_name(order_by_column_name), order_by_mode}},
      chunk_size, Sort::ForceMaterialization::Yes);
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

std::string get_table_name(const std::string table_name, const std::string order_by_column , const std::string encoding) {
  return table_name + "_orderby_" + order_by_column + "_encoding_" + encoding;
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
      std::ofstream segment_meta_data_csv_file(SEGMENT_META_DATA_FILE);
      segment_meta_data_csv_file << "TABLE_NAME,COLUMN_ID,ORDER_BY,ENCODING,CHUNK_ID,ROW_COUNT,SIZE_IN_BYTES\n";

      std::ofstream index_meta_data_csv_file(INDEX_META_DATA_FILE);
      index_meta_data_csv_file << "TABLE_NAME,COLUMN_ID,ORDER_BY,ENCODING,CHUNK_ID,ROW_COUNT,SIZE_IN_BYTES\n"; 
      
      // Sort table and add sorted tables to the storage manager
      // Load origninal table from tbl file with specified chunk size
      std::cout << "Load initial table form tbl file '" << TBL_FILE << "' with chunk size: " << CHUNK_SIZE << "." << std::endl;
      auto loaded_table = load_table(TBL_FILE, CHUNK_SIZE);

      for (const auto order_by_column : ORDER_COLUMNS) {
        for (const opossum::SegmentEncodingSpec & encoding : CHUNK_ENCODINGS) {

          // Maybe there is a better way to do this
          const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
          const auto new_table_name = get_table_name(TABLE_NAME_PREFIX, order_by_column, encoding_type);
          auto table_wrapper = std::make_shared<TableWrapper>(loaded_table);
          table_wrapper->execute();
          const auto chunk_encoding_spec = ChunkEncodingSpec(table_wrapper->get_output()->column_count(), {encoding});

          auto new_table = loaded_table;

          if (strcmp(order_by_column, "unsorted") == 0) {
            new_table = load_table(TBL_FILE, CHUNK_SIZE);
            ChunkEncoder::encode_all_chunks(new_table, chunk_encoding_spec);
          } else {
            new_table = sort_table_chunk_wise(loaded_table, order_by_column, CHUNK_SIZE, chunk_encoding_spec);
          }

          storage_manager.add_table(new_table_name, new_table);
          std::cout << "Created table: " << new_table_name << std::endl;

          // table stats
          for (auto column_id = ColumnID{0}; column_id < new_table->column_count(); ++column_id) {
            for (auto chunk_id = ChunkID{0}, end = new_table->chunk_count(); chunk_id < end;  ++chunk_id) {
              const auto& chunk = new_table->get_chunk(chunk_id);
              const auto& segment = chunk->get_segment(column_id);

              segment_meta_data_csv_file << new_table_name << "," << new_table->column_name(column_id) << ","<< order_by_column << ","<< encoding << ","<< chunk_id << "," << CHUNK_SIZE << "," << segment->memory_usage(MemoryUsageCalculationMode::Full) << "\n";
            }
          }

          // create index for each chunk and each segment 
          if (CREATE_INDEX && encoding.encoding_type == EncodingType::Dictionary) {
            std::cout << "Creating indexes: ";
            const auto chunk_count = new_table->chunk_count();
            const auto column_count = new_table->column_count();
              
            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
                const auto& index = new_table->get_chunk(chunk_id)->create_index<GroupKeyIndex>(std::vector<ColumnID>{column_id});
                index_meta_data_csv_file << new_table_name << "," << new_table->column_name(column_id) << ","<< order_by_column << ","<< encoding << ","<< chunk_id << "," << CHUNK_SIZE << "," << index->memory_consumption() << "\n";
              }
            }
            std::cout << "done " << std::endl;
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

BENCHMARK_DEFINE_F(PingDataMicroBenchmarkFixture, BM_Keven_OrderingLessThanEqualsPerformance)(benchmark::State& state) {
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_CAPTAIN_STATUS.size(), "Sample search values for columns should have the same length.");
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_LATITUDE.size(), "Sample search values for columns should have the same length.");
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_TIMESTAMP.size(), "Sample search values for columns should have the same length.");
  Assert(BM_VAL_TIMESTAMP.size() == BM_SCAN_VALUES, "Sample search values for columns should have the same length.");

  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto order_by_column = ORDER_COLUMNS[state.range(0)];
  const auto encoding = CHUNK_ENCODINGS[state.range(1)];
  const auto scan_column_index = state.range(2);
  const auto scan_column = SCAN_COLUMNS[scan_column_index];
  const auto search_value_index = state.range(3);


  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name(scan_column);
  //const auto order_by_column_id = table->column_id_by_name(order_by_column);
  auto operand = pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, scan_column);

  // scan
  std::shared_ptr<BinaryPredicateExpression> predicate;
  // should by nicer dicer
  if (scan_column_index == 0) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThanEquals, operand, value_(BM_VAL_CAPTAIN_ID[search_value_index]));}
  if (scan_column_index == 1) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThanEquals, operand, value_(BM_VAL_LATITUDE[search_value_index]));}
  if (scan_column_index == 2) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThanEquals, operand, value_(BM_VAL_LONGITUDE[search_value_index]));}
  if (scan_column_index == 3) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThanEquals, operand, value_(BM_VAL_TIMESTAMP[search_value_index]));}
  if (scan_column_index == 4) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThanEquals, operand, value_(BM_VAL_CAPTAIN_STATUS[search_value_index]));}

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto warm_up_table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_table_scan->execute();

  ////
  //// Assertions for the correction scan method, encoding types, and expected sort orders.
  ////
  // if (warm_up_table_scan->get_output()->row_count() == 0) {
  //   std::cout << "Warning: executed table scan did not yield any results." << std::endl;
  // }

  // if (order_by_column == scan_column) {
  //   const auto description = warm_up_table_scan->description(DescriptionMode::SingleLine);
  //   Assert(description.find("scanned with binary search]") != std::string::npos, "Requested a sorted scan on colum " + scan_column + ", but impl description is:\n\t" + description);
  //   Assert(description.find("ColumnVsValue") != std::string::npos, "Executed scan on colum " + scan_column + " was no ColumnVsValue scan.");
  // }
  
  // const auto chunk_count = table->chunk_count();
  // const auto column_count = table->column_count();
  // for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
  //   resolve_data_type(table->column_data_type(column_id), [&](auto type) {
  //     using ColumnDataType = typename decltype(type)::type;

  //     for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
  //       const auto segment = table->get_chunk(chunk_id)->get_segment(column_id);

  //         const auto unencoded_segment = std::dynamic_pointer_cast<const ValueSegment<ColumnDataType>>(segment);
  //         if (unencoded_segment) {
  //           Assert(encoding.encoding_type == EncodingType::Unencoded, "Encoding type >>Unencoded<< requested for " + scan_column + " but not found.");
  //         } else {
  //           const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
  //           Assert(encoded_segment && encoded_segment->encoding_type() == encoding.encoding_type, "Encoding type not as requested for " + scan_column + ".");
  //         }

  //         if (column_id == order_by_column_id) {
  //           Assert(table->get_chunk(chunk_id)->ordered_by()->first == ColumnID{column_id}, "Chunk is unsorted, but it should be.");
  //         } else {
  //           Assert(table->get_chunk(chunk_id)->ordered_by() == std::nullopt || table->get_chunk(chunk_id)->ordered_by()->first != ColumnID{column_id}, "Chunk shall not be sorted, but is.");
  //         }
  //     }
  //   });
  // }
  ////
  //// Assertions End
  ////
  
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(PingDataMicroBenchmarkFixture, BM_Keven_IndexScan)(benchmark::State& state) {
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_CAPTAIN_STATUS.size(), "Sample search values for columns should have the same length. 1");
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_LATITUDE.size(), "Sample search values for columns should have the same length. 2");
  Assert(BM_VAL_CAPTAIN_ID.size() == BM_VAL_TIMESTAMP.size(), "Sample search values for columns should have the same length. 3");
  Assert(BM_VAL_TIMESTAMP.size() == BM_SCAN_VALUES, "Sample search values for columns should have the same length. 4");

  auto& storage_manager = Hyrise::get().storage_manager;

  const auto order_by_column = ORDER_COLUMNS[state.range(0)];
  const auto encoding = CHUNK_ENCODINGS[state.range(1)];
  const auto scan_column_index = state.range(2);
  const auto scan_column = SCAN_COLUMNS[scan_column_index];
  const auto search_value_index = state.range(3);

  if (encoding.encoding_type != EncodingType::Dictionary) state.SkipWithError("Running only for dictionary encoding (others unsupported by the GroupKey index). Skipping others.");

  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name(scan_column);
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
    const auto index_scan = std::make_shared<IndexScan>(table_wrapper, SegmentIndexType::GroupKey, scan_column_ids, PredicateCondition::LessThanEquals, right_values);
    index_scan->execute();
  }
}

BENCHMARK_DEFINE_F(PingDataMicroBenchmarkFixture, BM_Keven_BetweenPerformance)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;

  const auto order_by_column = ORDER_COLUMNS[state.range(0)];
  const auto encoding = CHUNK_ENCODINGS[state.range(1)];
  const auto scan_column_index = state.range(2);
  const auto scan_column = SCAN_COLUMNS[scan_column_index];
  const auto search_value_index = state.range(3);

  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name(scan_column);
  const auto column_expression =  pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, scan_column);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  // setting up right value (i.e., the search value)
  std::shared_ptr<BetweenExpression> predicate;
  // should by nicer dicer
  if (scan_column_index == 0) { predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, column_expression, value_(BM_BETWEEN_VAL_CAPTAIN_ID[search_value_index][0]), value_(BM_BETWEEN_VAL_CAPTAIN_ID[search_value_index][1])); }
  if (scan_column_index == 1) { predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, column_expression, value_(BM_BETWEEN_VAL_LATITUDE[search_value_index][0]), value_(BM_BETWEEN_VAL_LATITUDE[search_value_index][1])); }
  if (scan_column_index == 2) { predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, column_expression, value_(BM_BETWEEN_VAL_LONGITUDE[search_value_index][0]), value_(BM_BETWEEN_VAL_LONGITUDE[search_value_index][1])); }
  if (scan_column_index == 3) { predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, column_expression, value_(BM_BETWEEN_VAL_TIMESTAMP[search_value_index][0]), value_(BM_BETWEEN_VAL_TIMESTAMP[search_value_index][1])); }
  if (scan_column_index == 4) { predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenInclusive, column_expression, value_(BM_BETWEEN_VAL_CAPTAIN_STATUS[search_value_index][0]), value_(BM_BETWEEN_VAL_CAPTAIN_STATUS[search_value_index][1])); }
  
  const auto warm_up_between_scan = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up_between_scan->execute();

  for (auto _ : state) {
    const auto between_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    between_scan->execute();
  }
}

BENCHMARK_DEFINE_F(PingDataMicroBenchmarkFixture, BM_Keven_BetweenIndexScan)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;

  const auto order_by_column = ORDER_COLUMNS[state.range(0)];
  const auto encoding = CHUNK_ENCODINGS[state.range(1)];
  const auto scan_column_index = state.range(2);
  const auto scan_column = SCAN_COLUMNS[scan_column_index];
  const auto search_value_index = state.range(3);

  if (encoding.encoding_type != EncodingType::Dictionary) state.SkipWithError("Running only for dictionary encoding (others unsupported by the GroupKey index). Skipping others.");

  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  const auto scan_column_id = table->column_id_by_name(scan_column);
  const auto column_expression =  pqp_column_(scan_column_id, table->column_data_type(scan_column_id), false, scan_column);

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  // setting up right value (i.e., the search value)
  std::vector<AllTypeVariant> right_values;
  std::vector<AllTypeVariant> right_values2;
  // should by nicer dicer
  if (scan_column_index == 0) { 
    right_values = {BM_BETWEEN_VAL_CAPTAIN_ID[search_value_index][0]}; 
    right_values2 = {BM_BETWEEN_VAL_CAPTAIN_ID[search_value_index][1]}; 
  }
  if (scan_column_index == 1) { 
    right_values = {BM_BETWEEN_VAL_LATITUDE[search_value_index][0]};
    right_values2 = {BM_BETWEEN_VAL_LATITUDE[search_value_index][1]}; 
  }
  if (scan_column_index == 2) { 
    right_values = {BM_BETWEEN_VAL_LONGITUDE[search_value_index][0]};
    right_values2 = {BM_BETWEEN_VAL_LONGITUDE[search_value_index][1]}; 
  }
  if (scan_column_index == 3) { 
    right_values = {BM_BETWEEN_VAL_TIMESTAMP[search_value_index][0]};
    right_values2 = {BM_BETWEEN_VAL_TIMESTAMP[search_value_index][1]};  
  }
  if (scan_column_index == 4) { 
    right_values = {BM_BETWEEN_VAL_CAPTAIN_STATUS[search_value_index][0]};
    right_values2 = {BM_BETWEEN_VAL_CAPTAIN_STATUS[search_value_index][1]};
  }

  
  const std::vector<ColumnID> scan_column_ids = {scan_column_id};
  for (auto _ : state) {
    const auto index_scan = std::make_shared<IndexScan>(table_wrapper, SegmentIndexType::GroupKey, scan_column_ids, PredicateCondition::BetweenInclusive, right_values, right_values2);
    index_scan->execute();
  }
}

static void CustomArguments(benchmark::internal::Benchmark* b) {
  for (size_t order_by_column_id = 0; order_by_column_id < ORDER_COLUMNS.size(); ++order_by_column_id) {
    for (size_t encoding_id = 0; encoding_id < CHUNK_ENCODINGS.size(); ++encoding_id) {
      for (size_t scan_column_id = 0; scan_column_id < SCAN_COLUMNS.size(); ++scan_column_id) {
        for (size_t scan_value_id = 0; scan_value_id < BM_SCAN_VALUES; ++scan_value_id)
        {
          b->Args({static_cast<long long>(order_by_column_id), static_cast<long long>(encoding_id), static_cast<long long>(scan_column_id), static_cast<long long>(scan_value_id)});
        }
      }
    }
  }
}
BENCHMARK_REGISTER_F(PingDataMicroBenchmarkFixture, BM_Keven_OrderingLessThanEqualsPerformance)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(PingDataMicroBenchmarkFixture, BM_Keven_IndexScan)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(PingDataMicroBenchmarkFixture, BM_Keven_BetweenPerformance)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(PingDataMicroBenchmarkFixture, BM_Keven_BetweenIndexScan)->Apply(CustomArguments);


}  // namespace opossum
