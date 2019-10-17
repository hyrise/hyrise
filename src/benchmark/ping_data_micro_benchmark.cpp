#include "micro_benchmark_basic_fixture.hpp"

#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"

#include "utils/load_table.hpp"
#include "operators/print.hpp"
#include "operators/sort.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {
using namespace opossum;

// benchmark seetings
constexpr auto TBL_FILE = "../../data/10mio_pings.tbl";
//constexpr auto TBL_FILE = "../../data/100_pings.tbl";
constexpr auto TABLE_NAME = "ping";
const auto CHUNK_SIZES = std::vector{size_t{1000000}};
//const auto CHUNK_SIZES = std::vector{size_t{10}};
const auto ORDER_COLUMNS = std::vector{"captain_id", "latitude", "timestamp", "captain_status"};
const auto CHUNK_ENCODINGS = std::vector{EncodingType::Unencoded, EncodingType::Dictionary, EncodingType::LZ4, EncodingType::RunLength};

// single value benchmark values (median, min, max)
// determined by column stats python script
const auto BM_SCAN_VALUES = 3;
const auto BM_VAL_CAPTAIN_ID = std::vector{298186, 59547, 1211286};
const auto BM_VAL_CAPTAIN_STATUS = std::vector{2, 1, 2};
const auto BM_VAL_LATITUDE = std::vector{25.1388267, 24.9154559, 25.1916198};
const auto BM_VAL_TIMESTAMP = std::vector{"2018-12-06 23:57:34", "2018-12-22 23:22:11", "2018-11-05 05:38:15"};

std::shared_ptr<Table> sort_table_chunk_wise(const std::shared_ptr<const Table>& input_table,
    const std::string order_by_column_name, const size_t chunk_size, const std::optional<ChunkEncodingSpec>& chunk_encoding_spec = std::nullopt,
    const OrderByMode order_by_mode = OrderByMode::Ascending) {
  // empty table to which we iteratively add the sorted chunks
  auto sorted_table = std::make_shared<Table>(input_table->column_definitions(), TableType::Data, chunk_size, UseMvcc::No);

  const auto chunk_count = input_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    // create new single chunk and create a new table with that chunk
    auto new_chunk = std::make_shared<Chunk>(input_table->get_chunk(chunk_id)->segments());
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
    sorted_table->append_chunk(immutable_sorted_table->get_chunk(ChunkID{0})->segments());
    const auto& added_chunk = sorted_table->get_chunk(chunk_id);
    added_chunk->set_ordered_by(std::make_pair(sorted_table->column_id_by_name(order_by_column_name), order_by_mode));

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

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class PingDataMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) {
    auto& storage_manager = Hyrise::get().storage_manager;

    // Generate tables
    if (!_data_generated) {
      
      // Sort table and add sorted tables to the storage manager
      for (const auto chunk_size : CHUNK_SIZES) {
        // Load origninal table from tbl file with specified chunk size
        std::cout << "Load initial table form tbl file with chunk size:" << chunk_size <<"."<< std::endl;
        auto loaded_table = load_table(TBL_FILE, chunk_size);

        //schould be removed 
        //storage_manager.add_table("pings", loaded_table);
        for (const auto order_by_column : ORDER_COLUMNS) {
          for (const auto encoding : CHUNK_ENCODINGS) {

            // Maybe there is a better way to do this
            const auto encoding_type = encoding_type_to_string.left.at(encoding);
            const auto sorted_table_name = get_table_name(TABLE_NAME, chunk_size, order_by_column, encoding_type);
            auto table_wrapper = std::make_shared<TableWrapper>(loaded_table);
            table_wrapper->execute();
            const auto chunk_encoding_spec = ChunkEncodingSpec(table_wrapper->get_output()->column_count(), {SegmentEncodingSpec{encoding}});

            auto sorted_table = sort_table_chunk_wise(loaded_table, order_by_column, chunk_size, chunk_encoding_spec);
            storage_manager.add_table(sorted_table_name, sorted_table);
            std::cout << "Created table: " << sorted_table_name << std::endl;
          }
        }
      }
    }

    _data_generated = true;
  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State&) {}

  inline static bool _data_generated = false;

};

static void BM_Ping_Print_Sorted_Tables(benchmark::State& state) {
  // debug print of unsorted table
  constexpr auto chunk_size = size_t{10};
  auto table = load_table("/Users/krichly/Desktop/100_pings.tbl", chunk_size);
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

BENCHMARK_DEFINE_F(PingDataMicroBenchmarkFixture, BM_Keven_OrderingGreaterThanEqualsPerformance)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto chunk_size = CHUNK_SIZES[state.range(0)];
  const auto order_by_column = ORDER_COLUMNS[state.range(1)];
  const auto encoding = CHUNK_ENCODINGS[state.range(2)];
  const auto sort_column = ORDER_COLUMNS[state.range(3)];


  const auto encoding_type = encoding_type_to_string.left.at(encoding);
  const auto table_name = get_table_name(TABLE_NAME, chunk_size, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  auto operand = pqp_column_(table->column_id_by_name(sort_column), table->column_data_type(table->column_id_by_name(sort_column)), false, sort_column);

  // scan
  std::shared_ptr<BinaryPredicateExpression> predicate;
  // should by nicer dicer
  if (state.range(3) == 0) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand, value_(BM_VAL_CAPTAIN_ID[state.range(4)]));}
  if (state.range(3) == 1) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand, value_(BM_VAL_LATITUDE[state.range(4)]));}
  if (state.range(3) == 2) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand, value_(BM_VAL_TIMESTAMP[state.range(4)]));}
  if (state.range(3) == 3) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals, operand, value_(BM_VAL_CAPTAIN_STATUS[state.range(4)]));}

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(PingDataMicroBenchmarkFixture, BM_Keven_OrderingEqualsPerformance)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;
  
  const auto chunk_size = CHUNK_SIZES[state.range(0)];
  const auto order_by_column = ORDER_COLUMNS[state.range(1)];
  const auto encoding = CHUNK_ENCODINGS[state.range(2)];
  const auto sort_column = ORDER_COLUMNS[state.range(3)];


  const auto encoding_type = encoding_type_to_string.left.at(encoding);
  const auto table_name = get_table_name(TABLE_NAME, chunk_size, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);

  auto operand = pqp_column_(table->column_id_by_name(sort_column), table->column_data_type(table->column_id_by_name(sort_column)), false, sort_column);

  // scan
  std::shared_ptr<BinaryPredicateExpression> predicate;
  // should by nicer dicer
  if (state.range(3) == 0) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_CAPTAIN_ID[state.range(4)]));}
  if (state.range(3) == 1) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_LATITUDE[state.range(4)]));}
  if (state.range(3) == 2) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_TIMESTAMP[state.range(4)]));}
  if (state.range(3) == 3) {predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, operand, value_(BM_VAL_CAPTAIN_STATUS[state.range(4)]));}

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  
  for (auto _ : state) {
    const auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
  }
}

static void CustomArguments(benchmark::internal::Benchmark* b) {
  for (std::size_t chunk_size_id = 0; chunk_size_id < CHUNK_SIZES.size(); ++chunk_size_id) {
    for (std::size_t order_by_column_id = 0; order_by_column_id < ORDER_COLUMNS.size(); ++order_by_column_id) {
      for (std::size_t encoding_id = 0; encoding_id < CHUNK_ENCODINGS.size(); ++encoding_id) {
        for (std::size_t scan_column_id = 0; scan_column_id < ORDER_COLUMNS.size(); ++scan_column_id) {
          for (std::size_t scan_value_id = 0; scan_value_id < BM_SCAN_VALUES; ++scan_value_id)
          {
            b->Args({static_cast<long long>(chunk_size_id),static_cast<long long>(order_by_column_id), static_cast<long long>(encoding_id), static_cast<long long>(scan_column_id), static_cast<long long>(scan_value_id)});
          }
        }
      }
    }
  }
}
BENCHMARK_REGISTER_F(PingDataMicroBenchmarkFixture, BM_Keven_OrderingGreaterThanEqualsPerformance)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(PingDataMicroBenchmarkFixture, BM_Keven_OrderingEqualsPerformance)->Apply(CustomArguments);

}  // namespace opossum