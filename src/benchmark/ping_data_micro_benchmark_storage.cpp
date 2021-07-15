// Include before Fail() is defined
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Woverflow"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wattributes"
#pragma GCC diagnostic pop

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

#include "memory/umap_memory_resource.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {
using namespace opossum;

///////////////////////////////
// benchmark seetings
///////////////////////////////

// input and output settings 
///////////////////////////////
constexpr auto SEGMENT_META_DATA_FILE = "../../out/10mio/segment_meta_data_int_index.csv";
constexpr auto INDEX_META_DATA_FILE = "../../out/10mio/index_meta_data_int_index.csv";
constexpr auto TBL_FILE = "../../data/10mio_pings_no_id_int.tbl";

// table and compression settings
///////////////////////////////
constexpr auto TABLE_NAME_PREFIX = "ping";
const auto CHUNK_SIZE = size_t{1'000'000};
const auto SCAN_COLUMNS = std::vector{"captain_id", "latitude", "longitude", "timestamp", "captain_status"};
const auto ORDER_COLUMNS = std::vector{"captain_id", "latitude", "longitude", "timestamp", "captain_status", "unsorted"};

// Frame of References supports only int columns
// Dictionary Encoding should always have the id 0
const auto CHUNK_ENCODINGS = std::vector{SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::Unencoded}, SegmentEncodingSpec{EncodingType::LZ4}, SegmentEncodingSpec{EncodingType::RunLength}, SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128}};
const auto CREATE_INDEX = true; 

// 10 mio pings table

// quantile benchmark values (int table)
// timestamp values --> unix timestamp
// [0.0001, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0]
///////////////////////////////
const auto BM_VAL_CAPTAIN_ID = std::vector{11, 507, 1021, 1909, 4593, 8817, 17926, 37285, 57997, 121885, 207423, 391081, 538892, 670817, 811067, 1157846, 1280358};
const auto BM_VAL_CAPTAIN_STATUS = std::vector{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2};
const auto BM_VAL_LATITUDE = std::vector{243921506, 249199220, 250004849, 250302032, 250451594, 250558091, 250683994, 250767202, 250834293, 251013562, 251229783, 251682671, 251944398, 252085778, 252179506, 252447268, 525276124};
const auto BM_VAL_LONGITUDE = std::vector{543605120, 550208361, 551151508, 551340698, 551411140, 551464705, 551527705, 551661225, 551769067, 552056083, 552420155, 552663696, 552803335, 552904369, 553155586, 553625274, 2100403189};
const auto BM_VAL_TIMESTAMP = std::vector{1541026998, 1541032124, 1541034919, 1541040924, 1541049411, 1541056831, 1541065715, 1541076481, 1541089685, 1541117270, 1541138605, 1541154132, 1541171747, 1541180035, 1541190288, 1541219269, 1541236836};

const auto BM_SCAN_VALUES = BM_VAL_CAPTAIN_ID.size();

// quantile between benchmark values (int table)
// timestamp values --> unix timestamp
// [0.0001, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0]
///////////////////////////////
const std::vector<std::vector<int>> BM_BETWEEN_VAL_CAPTAIN_ID {{207423, 208069}, {203688, 212469}, {198608, 217921}, {176038, 235600}, {159431, 259789}, {142036, 313442}, {121885, 391081}, {99640, 413556}, {80675, 444767}, {57997, 538892}, {37285, 670817}, {17926, 811067}, {8817, 931727}, {6250, 1103790}, {4593, 1157846}, {1909, 1209832}, {8, 1280358}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_CAPTAIN_STATUS {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 2}, {1, 2}, {1, 2}, {1, 2}, {1, 2}, {1, 2}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_LATITUDE {{251229635, 251229817}, {251205580, 251244877}, {251186382, 251258863}, {251156067, 251327418}, {251118305, 251435758}, {251076483, 251562689}, {251013562, 251682671}, {250957584, 251801594}, {250915520, 251867848}, {250834293, 251944398}, {250767202, 252085778}, {250683994, 252179506}, {250558091, 252303117}, {250495668, 252364041}, {250451594, 252447268}, {250302032, 252608756}, {-31439997, 525276124}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_LONGITUDE {{552419834, 552420525}, {552401293, 552446079}, {552375420, 552467950}, {552297534, 552509809}, {552205344, 552567678}, {552121165, 552621719}, {552056083, 552663696}, {551998586, 552712421}, {551937507, 552746462}, {551769067, 552803335}, {551661225, 552904369}, {551527705, 553155586}, {551464705, 553433091}, {551435500, 553528324}, {551411140, 553625274}, {551340698, 553989117}, {-1347527969, 2100403189}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_TIMESTAMP {{1541138598, 1541138612}, {1541137889, 1541139310}, {1541137112, 1541139993}, {1541134602, 1541141812}, {1541130169, 1541146881}, {1541125209, 1541150508}, {1541117270, 1541154132}, {1541106624, 1541158501}, {1541100595, 1541162844}, {1541089685, 1541171747}, {1541076481, 1541180035}, {1541065715, 1541190288}, {1541056831, 1541209600}, {1541053133, 1541215064}, {1541049411, 1541219269}, {1541040924, 1541227207}, {1541026811, 1541236836}};

// 400 mio pings table

// quantile benchmark values (int table)
// timestamp values --> unix timestamp
// [0.0001, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0]
///////////////////////////////
//const auto BM_VAL_CAPTAIN_ID = std::vector{4, 511, 1051, 2156, 5075, 11309, 26152, 51264, 71463, 153884, 261690, 444765, 681250, 830979, 951600, 1209929, 1419878};
//const auto BM_VAL_CAPTAIN_STATUS = std::vector{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2};
//const auto BM_VAL_LATITUDE = std::vector{243318475, 249128864, 249867308, 250278868, 250441577, 250552193, 250681596, 250763657, 250831026, 251008836, 251205836, 251695319, 251959544, 252079334, 252166598, 252442895, 601671321};
//const auto BM_VAL_LONGITUDE = std::vector{543464250, 550118701, 551136315, 551335287, 551413500, 551467322, 551533330, 551663587, 551771444, 552061945, 552444439, 552686763, 552805030, 552905709, 553158162, 553615144, 2137369825};
//const auto BM_VAL_TIMESTAMP = std::vector{1541029693, 1541117270, 1541190288, 1541473344, 1541871716, 1542301289, 1542701374, 1543132978, 1543613596, 1544417407, 1545152896, 1545879041, 1546772391, 1547148186, 1547525107, 1548196911, 1548975682};

//const auto BM_SCAN_VALUES = BM_VAL_CAPTAIN_ID.size();

// quantile between benchmark values (int table)
// timestamp values --> unix timestamp
// [0.0001, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0]
///////////////////////////////
//const std::vector<std::vector<int>> BM_BETWEEN_VAL_CAPTAIN_ID {{261690, 261690}, {257105, 265387}, {249105, 274829}, {232076, 313448}, {206962, 391070}, {172746, 413552}, {153884, 444765}, {135981, 491021}, {111718, 538905}, {71463, 681250}, {51264, 830979}, {26152, 951600}, {11309, 1157854}, {7739, 1188073}, {5075, 1209929}, {2156, 1267757}, {4, 1419878}};
//const std::vector<std::vector<int>> BM_BETWEEN_VAL_CAPTAIN_STATUS {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 2}, {1, 2}, {1, 2}, {1, 2}, {1, 2}};
//const std::vector<std::vector<int>> BM_BETWEEN_VAL_LATITUDE {{251205594, 251206088}, {251186996, 251229239}, {251170200, 251244361}, {251150524, 251307385}, {251111686, 251424324}, {251063392, 251567452}, {251008836, 251695319}, {250959444, 251817667}, {250916446, 251875782}, {250831026, 251959544}, {250763657, 252079334}, {250681596, 252166598}, {250552193, 252294631}, {250485765, 252349413}, {250441577, 252442895}, {250278868, 252584668}, {-348142571, 601671321}};
//const std::vector<std::vector<int>> BM_BETWEEN_VAL_LONGITUDE {{552444244, 552444647}, {552421152, 552466309}, {552403773, 552484039}, {552334887, 552512683}, {552219192, 552585334}, {552133587, 552638424}, {552061945, 552686763}, {552006222, 552727166}, {551944417, 552758956}, {551771444, 552805030}, {551663587, 552905709}, {551533330, 553158162}, {551467322, 553438739}, {551440083, 553526687}, {551413500, 553615144}, {551335287, 553972207}, {-2144769047, 2137369825}};
//const std::vector<std::vector<int>> BM_BETWEEN_VAL_TIMESTAMP {{1545152588, 1545153213}, {1545125220, 1545195923}, {1545101636, 1545220279}, {1544986222, 1545315256}, {1544806072, 1545537953}, {1544611869, 1545697161}, {1544417407, 1545879041}, {1544199073, 1546078740}, {1544001704, 1546335968}, {1543613596, 1546772391}, {1543132978, 1547148186}, {1542701374, 1547525107}, {1542301289, 1547873717}, {1542094512, 1548046293}, {1541871716, 1548196911}, {1541473344, 1548555299}, {1541026811, 1548975682}};

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

    // use umap
    //for (auto column_id = ColumnID{0}; column_id < immutable_sorted_table->column_count(); ++column_id) {
    //  const auto& segment = immutable_sorted_table->get_chunk(chunk_id)->get_segment(column_id);
    //  auto resource = new UmapMemoryResource(STORAGE);
    //  auto allocator = PolymorphicAllocator<void>{resource};

    //  const auto migrated_segment = segment->copy_using_allocator(allocator);
    //  immutable_sorted_table->get_chunk(chunk_id)->replace_segment(column_id, migrated_segment);
    // }

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
class PingDataStorageMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    auto& storage_manager = Hyrise::get().storage_manager;

    // Generate tables
    if (!_data_generated) {

      // umap resource 
      static auto global_umap_resource = new UmapMemoryResource("global");
      (void) global_umap_resource;

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

          // use umap
          for (auto chunk_id = ChunkID{0}; chunk_id < new_table->chunk_count(); ++chunk_id) {
            for (auto column_id = ColumnID{0}; column_id < new_table->column_count(); ++column_id) {
              const auto& segment = new_table->get_chunk(chunk_id)->get_segment(column_id);

              auto resource = global_umap_resource;
              auto allocator = PolymorphicAllocator<void>{resource};

              const auto migrated_segment = segment->copy_using_allocator(allocator);
              new_table->get_chunk(chunk_id)->replace_segment(column_id, migrated_segment);
            }
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

                // use umap
                auto resource = global_umap_resource;
            	auto allocator = PolymorphicAllocator<void>{resource};

                const auto  migrated_index = index->copy_using_allocator(allocator);
                new_table->get_chunk(chunk_id)->replace_index(index, migrated_index);

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
  void TearDown(::benchmark::State&) override {}

  inline static bool _data_generated = false;

};

///////////////////////////////
// benchmarks
///////////////////////////////

BENCHMARK_DEFINE_F(PingDataStorageMicroBenchmarkFixture, BM_STORAGE_Keven_OrderingLessThanEqualsPerformance)(benchmark::State& state) {
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

BENCHMARK_DEFINE_F(PingDataStorageMicroBenchmarkFixture, BM_STORAGE_Keven_IndexScan)(benchmark::State& state) {
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
  std::vector<ChunkID> indexed_chunks;
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    indexed_chunks.emplace_back(chunk_id);
  }

  for (auto _ : state) {
    const auto index_scan = std::make_shared<IndexScan>(table_wrapper, SegmentIndexType::GroupKey, scan_column_ids, PredicateCondition::LessThanEquals, right_values);
    index_scan->included_chunk_ids = indexed_chunks;
    index_scan->execute();
  }
}

BENCHMARK_DEFINE_F(PingDataStorageMicroBenchmarkFixture, BM_STORAGE_Keven_BetweenPerformance)(benchmark::State& state) {
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

BENCHMARK_DEFINE_F(PingDataStorageMicroBenchmarkFixture, BM_STORAGE_Keven_BetweenIndexScan)(benchmark::State& state) {
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
  std::vector<ChunkID> indexed_chunks;
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    indexed_chunks.emplace_back(chunk_id);
  }

  for (auto _ : state) {
    const auto index_scan = std::make_shared<IndexScan>(table_wrapper, SegmentIndexType::GroupKey, scan_column_ids, PredicateCondition::BetweenInclusive, right_values, right_values2);
    index_scan->included_chunk_ids = indexed_chunks;
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
BENCHMARK_REGISTER_F(PingDataStorageMicroBenchmarkFixture, BM_STORAGE_Keven_OrderingLessThanEqualsPerformance)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(PingDataStorageMicroBenchmarkFixture, BM_STORAGE_Keven_IndexScan)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(PingDataStorageMicroBenchmarkFixture, BM_STORAGE_Keven_BetweenPerformance)->Apply(CustomArguments);
BENCHMARK_REGISTER_F(PingDataStorageMicroBenchmarkFixture, BM_STORAGE_Keven_BetweenIndexScan)->Apply(CustomArguments);

}  // namespace opossum
