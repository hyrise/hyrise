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
#include "storage/index/group_key/composite_group_key_index.hpp"

#include "utils/assert.hpp"
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
constexpr auto INDEX_META_DATA_FILE = "../../out/400mio/index_meta_data_multi_index.csv";
constexpr auto TBL_FILE = "../../data/10mio_pings_no_id_int.tbl";

// table and compression settings
///////////////////////////////
constexpr auto TABLE_NAME_PREFIX = "ping";
const auto CHUNK_SIZE = size_t{40'000'000};
const auto SCAN_COLUMNS = std::vector{"captain_id", "latitude", "longitude", "timestamp", "captain_status"};
//const auto ORDER_COLUMNS = std::vector{"captain_id", "latitude", "longitude", "timestamp", "captain_status", "unsorted"};
const auto ORDER_COLUMNS = std::vector{"unsorted"};

std::vector<std::vector<std::tuple<ColumnID, int32_t, int32_t>>> WORKLOAD{
                                                                          {{ColumnID{0}, 0, 4},
                                                                           {ColumnID{4}, 0, 1}
                                                                          },
                                                                          {{ColumnID{1}, 251111686, 251424324},
                                                                           {ColumnID{2}, 552219192, 552585334},
                                                                           {ColumnID{3}, 1544417407, 1545879041},
                                                                           {ColumnID{4}, 0, 1}
                                                                          },
                                                                          {{ColumnID{0}, 0, 511},
                                                                           {ColumnID{1}, 250916446, 251875782},
                                                                           {ColumnID{2}, 551944417, 552758956}
                                                                          }};

const std::vector<std::vector<int>> MULTI_COLUMN_INDEXES {{0, 4}, {1, 2}};
std::map<std::pair<std::vector<int>, ChunkID>, std::shared_ptr<AbstractIndex>> multi_indexes; 
//const std::vector<int> SCAN_VALUES = {4, 1, 1, 1, 1};
const auto INDEX_VALUES = MULTI_COLUMN_INDEXES.size();
const auto INDEX_LENGTH = 2;

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
    const SortMode sort_mode = SortMode::Ascending) {
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
        SortColumnDefinition{single_chunk_table->column_id_by_name(order_by_column_name), sort_mode}},
      chunk_size, Sort::ForceMaterialization::Yes);
    sort->execute();
    const auto immutable_sorted_table = sort->get_output();

    // add sorted chunk to output table
    // Note: we do not care about MVCC at all at the moment
    sorted_table->append_chunk(get_segments_of_chunk(immutable_sorted_table, ChunkID{0}));
  
    const auto& added_chunk = sorted_table->get_chunk(chunk_id);
    added_chunk->finalize();
    added_chunk->set_individually_sorted_by(SortColumnDefinition(sorted_table->column_id_by_name(order_by_column_name), sort_mode));

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
class PingDataMultiIndexBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    auto& storage_manager = Hyrise::get().storage_manager;

    // Generate tables
    if (!_data_generated) {

      // file for table stats

      std::ofstream index_meta_data_csv_file(INDEX_META_DATA_FILE);
      index_meta_data_csv_file << "TABLE_NAME,COLUMN_ID,SECOND_COLUMN_ID,ORDER_BY,ENCODING,CHUNK_ID,ROW_COUNT,SIZE_IN_BYTES\n"; 
      
      // Sort table and add sorted tables to the storage manager
      // Load origninal table from tbl file with specified chunk size
      std::cout << "Load initial table form tbl file '" << TBL_FILE << "' with chunk size: " << CHUNK_SIZE << "." << std::endl;
      auto loaded_table = load_table(TBL_FILE, CHUNK_SIZE);

      const auto encoding = SegmentEncodingSpec{EncodingType::Dictionary};
      const auto encoding_name= encoding_type_to_string.left.at(encoding.encoding_type);

      for (const auto order_by_column : ORDER_COLUMNS) {

        const auto new_table_name = get_table_name(TABLE_NAME_PREFIX, order_by_column, encoding_name);
        
        auto table_wrapper = std::make_shared<TableWrapper>(loaded_table);
        table_wrapper->execute();
        const auto chunk_encoding_spec = ChunkEncodingSpec(table_wrapper->get_output()->column_count(), encoding);

        auto new_table = loaded_table;

        if (strcmp(order_by_column, "unsorted") == 0) {
          new_table = load_table(TBL_FILE, CHUNK_SIZE);
          ChunkEncoder::encode_all_chunks(new_table, chunk_encoding_spec);
        } else {
          new_table = sort_table_chunk_wise(loaded_table, order_by_column, CHUNK_SIZE, chunk_encoding_spec);
        }

        storage_manager.add_table(new_table_name, new_table);
        std::cout << "Created table: " << new_table_name << std::endl;

        // create index for each chunk and each segment 
        if (encoding.encoding_type == EncodingType::Dictionary) {
          for (size_t index_config_id = 0; index_config_id < INDEX_VALUES; ++index_config_id) {

            std::cout << "Creating indexes: ";

            const auto chunk_count = new_table->chunk_count();
            auto column_ids = std::vector<ColumnID>{};

            for (const auto& index_column : MULTI_COLUMN_INDEXES[index_config_id]) {
              column_ids.emplace_back(index_column);
            }
              
            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto& index = new_table->get_chunk(chunk_id)->create_index<CompositeGroupKeyIndex>(column_ids);
              multi_indexes.insert({{MULTI_COLUMN_INDEXES[index_config_id], chunk_id}, index});
              index_meta_data_csv_file << new_table_name << "," << new_table->column_name(column_ids[0]) << "," << new_table->column_name(column_ids[1]) << "," << order_by_column << ","<< encoding << ","<< chunk_id << "," << CHUNK_SIZE << "," << index->memory_consumption() << "\n";
            }
            std::cout << "done " << std::endl;
          }
        }
      }

      index_meta_data_csv_file.close();
    }

    _data_generated = true;
  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State&) override {}

  inline static bool _data_generated = false;

};

///////////////////////////////
// benchmarks
///////////////////////////////

/*
BENCHMARK_DEFINE_F(PingDataMultiIndexBenchmarkFixture, BM_MultiColumnIndexScan)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;

  const auto order_by_column = ORDER_COLUMNS[state.range(0)];
  const auto index_config = MULTI_COLUMN_INDEXES[state.range(1)];
  const auto search_value_index = state.range(2);
  const auto scan_op = state.range(3);

  const auto encoding = SegmentEncodingSpec{EncodingType::Dictionary};
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  auto scan_column_ids = std::vector<ColumnID>{};
  if (scan_op == 0) {
    scan_column_ids.emplace_back(index_config[0]);
  } else {
    for (const auto& index_column : index_config) {
      scan_column_ids.emplace_back(index_column);
    }
  }

  // setting up right value (i.e., the search value)
  std::vector<AllTypeVariant> right_values;
  for (const auto& scan_column_index : scan_column_ids) {
    if (scan_column_index == 0) { right_values.emplace_back(BM_VAL_CAPTAIN_ID[search_value_index]); }
    if (scan_column_index == 1) { right_values.emplace_back(BM_VAL_LATITUDE[search_value_index]); }
    if (scan_column_index == 2) { right_values.emplace_back(BM_VAL_LONGITUDE[search_value_index]); }
    if (scan_column_index == 3) { right_values.emplace_back(BM_VAL_TIMESTAMP[search_value_index]); }
    if (scan_column_index == 4) { right_values.emplace_back(BM_VAL_CAPTAIN_STATUS[search_value_index]); }
  }

  std::vector<ChunkID> indexed_chunks;
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    indexed_chunks.emplace_back(chunk_id);
  }

  for (auto _ : state) {
    const auto index_scan = std::make_shared<IndexScan>(table_wrapper, SegmentIndexType::CompositeGroupKey, scan_column_ids, PredicateCondition::LessThanEquals, right_values);
    index_scan->included_chunk_ids = indexed_chunks;
    index_scan->execute();
  }
}
*/

BENCHMARK_DEFINE_F(PingDataMultiIndexBenchmarkFixture, BM_MultiColumnIndexScan_04)(benchmark::State& state) {
  const auto multi_column_index_id = state.range(0);
  const auto query_id = state.range(1);

  const auto& multi_column_def = MULTI_COLUMN_INDEXES[multi_column_index_id];
  const auto& query = WORKLOAD[query_id];

  //std::cout << "Multi column index on [ ";
  //for (const auto column_id : multi_column_def) {
  //  std::cout << column_id << " ";
  //}
  //std::cout << " ]" << std::endl;

  std::cout << "############ Index: ";
  for (const auto& column_id : multi_column_def) std::cout << column_id << " ";
  std::cout << "\t\t  Query: ";
  for (const auto& scan : query) std::cout << std::get<0>(scan) << " ";
  std::cout << std::endl;

  std::vector<AllTypeVariant> lower_bounds;
  std::vector<AllTypeVariant> upper_bounds;

  // Starting from the index' first column, check how many scans we can cover.
  auto found_match = false;
  for (const auto column_id : multi_column_def) {
    // For every scan column, check if it is the currently checked index column. If so, append search values and continue
    for (const auto& scan : query) {
      const auto scan_column_id = std::get<0>(scan);
      if (column_id == scan_column_id) {
        const auto lower_bound = std::get<1>(scan);
        const auto upper_bound = std::get<2>(scan);

        found_match = true;
        lower_bounds.push_back(lower_bound);
        upper_bounds.push_back(upper_bound);
      }
    }

    if (!found_match) {
      // If the currently scan column is not part of the index, neglect all other scans
      // (composite index has to be used from start without "holes"). 
      break;
    }
  }

  //std::cout << "For the current query, we found the following search values:\n\tlower: ";
  //for (const auto lower : lower_bounds) std::cout << lower << " ";
  //std::cout << "\n\tupper: ";
  //for (const auto upper : upper_bounds) std::cout << upper << " ";
  //std::cout << std::endl;

  Assert(lower_bounds.size() == upper_bounds.size(), "Narf");
  
  auto& storage_manager = Hyrise::get().storage_manager;

  if (!lower_bounds.empty()) {
    for (auto _ : state) {
      const auto& table = storage_manager.get_table("ping_orderby_unsorted_encoding_Dictionary");
      const auto chunk_count = table->chunk_count();
      auto position_list = std::vector<RowID>{};
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto& index = multi_indexes[{multi_column_def, chunk_id}];
        auto range_begin = index->lower_bound(lower_bounds);
        auto range_end = index->upper_bound(upper_bounds);
 
        position_list.reserve(std::max(position_list.capacity(),
                              static_cast<size_t>(std::distance(range_begin, range_end))));
        for (; range_begin < range_end; ++range_begin) {
          position_list.emplace_back(chunk_id, *range_begin);
        }
      }
    }
  }
}

static void MultiIndexCustomArguments(benchmark::internal::Benchmark* b) {
  for (auto index_index = int64_t{0}; index_index < static_cast<int64_t>(MULTI_COLUMN_INDEXES.size()); ++index_index) {
    for (auto query_index = int64_t{0}; query_index < static_cast<int64_t>(WORKLOAD.size()); ++query_index) {
      b->Args({index_index, query_index});
    }
  }
}

BENCHMARK_REGISTER_F(PingDataMultiIndexBenchmarkFixture, BM_MultiColumnIndexScan_04)->Apply(MultiIndexCustomArguments);
//BENCHMARK_REGISTER_F(PingDataMultiIndexBenchmarkFixture, BM_MultiColumnIndexScan)->Apply(MultiIndexCustomArguments);

}  // namespace opossum
