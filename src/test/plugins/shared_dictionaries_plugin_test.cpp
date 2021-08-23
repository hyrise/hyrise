#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "lib/utils/plugin_test_utils.hpp"

#include "../../plugins/shared_dictionaries_plugin/shared_dictionaries_column_processor.hpp"

namespace opossum {

class SharedDictionariesPluginTest : public BaseTest {
 public:
  void SetUp() override {}

  void TearDown() override { Hyrise::reset(); }

 protected:
  const std::vector<std::string> _table_names{"part", "partsupp", "customer", "orders"};
  const std::string _tables_path{"resources/test_data/tbl/tpch/sf-0.001/"};
  const std::string _table_extension{".tbl"};
  const size_t _chunk_size = 16;

  const ColumnID _market_column_id{6};
  const std::string _market_column_name{"c_mktsegment"};

  void _add_all_test_tables() {
    auto& sm = Hyrise::get().storage_manager;
    if (!sm.has_table(_table_names[0])) {
      for (const auto& table_name : _table_names) {
        const auto table = load_table(_get_table_path(table_name), _chunk_size);
        _encode_table(table);
        sm.add_table(table_name, table);
      }
    }
  }

  void _validate_all_test_tables() {
    auto& sm = Hyrise::get().storage_manager;
    for (const auto& table_name : _table_names) {
      EXPECT_TABLE_EQ_ORDERED(sm.get_table(table_name), load_table(_get_table_path(table_name), _chunk_size));
    }
  }

  void _add_customer_table() {
    auto& sm = Hyrise::get().storage_manager;
    if (!sm.has_table("customer")) {
      const auto table = load_table(_get_table_path("customer"), _chunk_size);
      _encode_table(table);
      sm.add_table("customer", table);
    }
  }

  void _validate_customer_table() {
    auto& sm = Hyrise::get().storage_manager;
    EXPECT_TABLE_EQ_ORDERED(sm.get_table("customer"), load_table(_get_table_path("customer"), _chunk_size));
  }

  void _encode_table(std::shared_ptr<Table> table) {
    auto chunk_encoding_spec = ChunkEncodingSpec{};
    for (const auto& column_definition : table->column_definitions()) {
      if (encoding_supports_data_type(EncodingType::Dictionary, column_definition.data_type)) {
        chunk_encoding_spec.emplace_back(EncodingType::Dictionary);
      } else {
        chunk_encoding_spec.emplace_back(EncodingType::Unencoded);
      }
    }

    ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);
  }

  std::shared_ptr<Table> _get_customer_table() {
    auto& sm = Hyrise::get().storage_manager;
    return sm.get_table("customer");
  }

  const SegmentChunkPair<pmr_string> _get_segment_chunk_pair() {
    const auto chunk = _get_customer_table()->get_chunk(ChunkID{0});
    const auto segment = chunk->get_segment(_market_column_id);
    const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<pmr_string>>(segment);
    return std::make_pair(dictionary_segment, chunk);
  }

  std::string _get_table_path(const std::string& table_name) { return _tables_path + table_name + _table_extension; }

  static float _calc_jaccard_index(size_t union_size, size_t intersection_size) {
    return SharedDictionariesColumnProcessor<int32_t>::_calc_jaccard_index(union_size, intersection_size);
  }

  static void _initialize_merge_plans(
      SharedDictionariesColumnProcessor<pmr_string>& column_processor,
      std::vector<std::shared_ptr<SharedDictionariesColumnProcessor<pmr_string>::MergePlan>>& merge_plans) {
    column_processor._initialize_merge_plans(merge_plans);
  }

  static bool _should_merge(SharedDictionariesColumnProcessor<pmr_string>& column_processor, const float jaccard_index,
                            const size_t current_dictionary_size, const size_t shared_dictionary_size,
                            const std::vector<SegmentChunkPair<pmr_string>>& shared_segment_chunk_pairs) {
    return column_processor._should_merge(jaccard_index, current_dictionary_size, shared_dictionary_size,
                                          shared_segment_chunk_pairs);
  }
};

TEST_F(SharedDictionariesPluginTest, LoadUnloadPluginEmpty) {
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libhyriseSharedDictionariesPlugin"));
  pm.unload_plugin("hyriseSharedDictionariesPlugin");
}

TEST_F(SharedDictionariesPluginTest, LoadUnloadPlugin) {
  _add_all_test_tables();
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libhyriseSharedDictionariesPlugin"));
  _validate_all_test_tables();
  pm.unload_plugin("hyriseSharedDictionariesPlugin");
}

TEST_F(SharedDictionariesPluginTest, ReloadPlugin) {
  _add_all_test_tables();
  auto& pm = Hyrise::get().plugin_manager;

  pm.load_plugin(build_dylib_path("libhyriseSharedDictionariesPlugin"));
  _validate_all_test_tables();
  pm.unload_plugin("hyriseSharedDictionariesPlugin");

  pm.load_plugin(build_dylib_path("libhyriseSharedDictionariesPlugin"));
  _validate_all_test_tables();
  pm.unload_plugin("hyriseSharedDictionariesPlugin");
}

TEST_F(SharedDictionariesPluginTest, ProcessColumn) {
  _add_customer_table();
  auto stats = SharedDictionariesPlugin::SharedDictionariesStats{};
  auto column_processor = SharedDictionariesColumnProcessor<pmr_string>{
      _get_customer_table(), "customer", _market_column_id, _market_column_name, 0.1f, stats};
  column_processor.process();

  EXPECT_GT(stats.total_bytes_saved, 0);
  EXPECT_GT(stats.total_previous_bytes, 0);
  EXPECT_GT(stats.modified_previous_bytes, 0);
  EXPECT_GT(stats.num_merged_dictionaries, 0);
  EXPECT_GT(stats.num_shared_dictionaries, 0);
  EXPECT_EQ(stats.num_existing_shared_dictionaries, 0);
  EXPECT_EQ(stats.num_existing_merged_dictionaries, 0);

  _validate_customer_table();

  // Check if dictionary segments are modified
  auto check_successful = false;
  std::shared_ptr<const pmr_vector<pmr_string>> found_shared_dictionary = nullptr;

  const auto table = _get_customer_table();
  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    const auto segment = chunk->get_segment(_market_column_id);
    const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<pmr_string>>(segment);
    if (dictionary_segment && dictionary_segment->uses_dictionary_sharing()) {
      if (found_shared_dictionary) {
        // Search for second segment with same shared dictionary
        if (dictionary_segment->dictionary() == found_shared_dictionary) {
          check_successful = true;
          break;
        }
      } else {
        // Set first found shared dictionary
        found_shared_dictionary = dictionary_segment->dictionary();
      }
    }
  }

  EXPECT_TRUE(check_successful);
}

TEST_F(SharedDictionariesPluginTest, InitializeMergePlansEmpty) {
  _add_customer_table();
  auto stats = SharedDictionariesPlugin::SharedDictionariesStats{};
  auto column_processor = SharedDictionariesColumnProcessor<pmr_string>{
      _get_customer_table(), "customer", _market_column_id, _market_column_name, 0.1f, stats};

  auto merge_plans = std::vector<std::shared_ptr<SharedDictionariesColumnProcessor<pmr_string>::MergePlan>>{};
  _initialize_merge_plans(column_processor, merge_plans);
  EXPECT_EQ(merge_plans.size(), 0);
  EXPECT_EQ(stats.num_existing_merged_dictionaries, 0);
  EXPECT_EQ(stats.num_existing_shared_dictionaries, 0);
}

TEST_F(SharedDictionariesPluginTest, InitializeMergePlansNonEmpty) {
  _add_customer_table();

  auto stats_1 = SharedDictionariesPlugin::SharedDictionariesStats{};
  auto column_processor_1 = SharedDictionariesColumnProcessor<pmr_string>{
      _get_customer_table(), "customer", _market_column_id, _market_column_name, 0.1f, stats_1};
  column_processor_1.process();

  auto stats_2 = SharedDictionariesPlugin::SharedDictionariesStats{};
  auto column_processor_2 = SharedDictionariesColumnProcessor<pmr_string>{
      _get_customer_table(), "customer", _market_column_id, _market_column_name, 0.1f, stats_2};

  auto merge_plans = std::vector<std::shared_ptr<SharedDictionariesColumnProcessor<pmr_string>::MergePlan>>{};
  _initialize_merge_plans(column_processor_2, merge_plans);

  EXPECT_EQ(merge_plans.size(), stats_1.num_shared_dictionaries);
  EXPECT_EQ(stats_2.num_existing_merged_dictionaries, stats_1.num_merged_dictionaries);
  EXPECT_EQ(stats_2.num_existing_shared_dictionaries, stats_1.num_shared_dictionaries);

  EXPECT_GT(stats_2.num_existing_shared_dictionaries, 0);
  EXPECT_GT(stats_2.num_existing_merged_dictionaries, 0);
}

TEST_F(SharedDictionariesPluginTest, ShouldMerge) {
  _add_customer_table();
  auto stats = SharedDictionariesPlugin::SharedDictionariesStats{};
  auto column_processor = SharedDictionariesColumnProcessor<pmr_string>{
      _get_customer_table(), "customer", _market_column_id, _market_column_name, 0.5, stats};

  // Jaccard-Index threshold not reached
  EXPECT_FALSE(_should_merge(column_processor, 0.4f, 0, 0, {}));

  // Increases attribute vector width of current segment
  EXPECT_FALSE(_should_merge(column_processor, 0.6f, 1, 255, {}));

  // Increases attribute vector width of other shared segments
  EXPECT_FALSE(_should_merge(column_processor, 0.6f, 10000000, 255, {_get_segment_chunk_pair()}));

  // Does none of the above
  EXPECT_TRUE(_should_merge(column_processor, 0.6f, 1, 254, {_get_segment_chunk_pair()}));
}

TEST_F(SharedDictionariesPluginTest, AddNonMergedSegmentChunkPair) {
  _add_customer_table();
  const auto shared_dictionary = std::make_shared<pmr_vector<pmr_string>>();
  auto merge_plan = SharedDictionariesColumnProcessor<pmr_string>::MergePlan{shared_dictionary};
  merge_plan.add_segment_chunk_pair(_get_segment_chunk_pair(), false);
  EXPECT_TRUE(merge_plan.contains_non_merged_segment);
  EXPECT_GT(merge_plan.non_merged_dictionary_bytes, 0);
  EXPECT_GT(merge_plan.non_merged_total_bytes, 0);
  EXPECT_EQ(merge_plan.segment_chunk_pairs_to_merge.size(), 1);
}

TEST_F(SharedDictionariesPluginTest, AddMergedSegmentChunkPair) {
  _add_customer_table();
  const auto shared_dictionary = std::make_shared<pmr_vector<pmr_string>>();
  auto merge_plan = SharedDictionariesColumnProcessor<pmr_string>::MergePlan{shared_dictionary};
  merge_plan.add_segment_chunk_pair(_get_segment_chunk_pair(), true);
  EXPECT_TRUE(merge_plan.contains_already_merged_segment);
  EXPECT_EQ(merge_plan.segment_chunk_pairs_to_merge.size(), 1);
}

TEST_F(SharedDictionariesPluginTest, JaccardIndex) {
  EXPECT_EQ(_calc_jaccard_index(0, 0), 0.0f);
  EXPECT_EQ(_calc_jaccard_index(100, 50), 0.5f);
  if (HYRISE_DEBUG) {
    EXPECT_THROW(_calc_jaccard_index(1, 2), std::exception);
  }
}

}  // namespace opossum
