#include "base_test.hpp"

#include "operators/get_table.hpp"
#include "utils/meta_tables/meta_cached_operators_table.hpp"
#include "utils/meta_tables/meta_cached_queries_table.hpp"

namespace opossum {

class MetaCacheTablesTest : public BaseTest {
 public:
  static std::vector<std::shared_ptr<AbstractMetaTable>> meta_tables() {
    return {std::make_shared<MetaCachedQueriesTable>(), std::make_shared<MetaCachedOperatorsTable>()};
  }

 protected:
  std::shared_ptr<AbstractOperator> get_table;
  std::shared_ptr<Table> queries_table;
  std::shared_ptr<Table> operators_table;
  std::vector<AllTypeVariant> queries_values;
  std::vector<AllTypeVariant> operators_values;

#ifdef __GLIBCXX__
  const pmr_string query_hash = pmr_string{"32d82bf8ed3dba39"};
#elif _LIBCPP_VERSION
  const pmr_string query_hash = pmr_string{"3a912f483a4ece31"};
#else
  static_assert(false, "Unknown c++ library");
#endif

  void SetUp() {
    Hyrise::reset();
    auto& storage_manager = Hyrise::get().storage_manager;

    const auto int_int = load_table("resources/test_data/tbl/int_int.tbl", 10);
    storage_manager.add_table("int_int", int_int);

    get_table = std::make_shared<GetTable>("int_int");
    get_table->execute();

    queries_table = std::make_shared<Table>(TableColumnDefinitions{{"hash_value", DataType::String, false},
                                                                   {"frequency", DataType::Int, false},
                                                                   {"sql_string", DataType::String, false}},
                                            TableType::Data);
    queries_values = {query_hash, 1, pmr_string{"abc"}};

    operators_table = std::make_shared<Table>(TableColumnDefinitions{{"operator", DataType::String, false},
                                                                     {"query_hash", DataType::String, false},
                                                                     {"description", DataType::String, false},
                                                                     {"walltime_ns", DataType::Long, false},
                                                                     {"output_chunks", DataType::Long, false},
                                                                     {"output_rows", DataType::Long, false}},
                                              TableType::Data);
    operators_values = {pmr_string{get_table->name()},
                        query_hash,
                        pmr_string{get_table->description()},
                        static_cast<int64_t>(get_table->performance_data().walltime.count()),
                        static_cast<int64_t>(1),
                        static_cast<int64_t>(3)};

    Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
    Hyrise::get().default_pqp_cache->set("abc", get_table);
  }

  void TearDown() { Hyrise::reset(); }

  const std::shared_ptr<Table> generate_meta_table(const std::shared_ptr<AbstractMetaTable>& table) const {
    return table->_generate();
  }

  std::shared_ptr<Table> reference_meta_table(const std::string& name) {
    if (name == "cached_queries") return queries_table;
    return operators_table;
  }

  std::vector<AllTypeVariant> reference_meta_table_values(const std::string& name) {
    if (name == "cached_queries") return queries_values;
    return operators_values;
  }
};

class MultiCacheMetaTablesTest : public MetaCacheTablesTest,
                                 public ::testing::WithParamInterface<std::shared_ptr<AbstractMetaTable>> {};

auto meta_cache_table_test_formatter = [](const ::testing::TestParamInfo<std::shared_ptr<AbstractMetaTable>> info) {
  auto stream = std::stringstream{};
  stream << info.param->name();

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !(std::isalnum(c) || c == '_'); }),
               string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(MetaTable, MultiCacheMetaTablesTest, ::testing::ValuesIn(MetaCacheTablesTest::meta_tables()),
                         meta_cache_table_test_formatter);

TEST_P(MultiCacheMetaTablesTest, MetaTableGeneration) {
  const auto expected_table = reference_meta_table(GetParam()->name());
  const auto meta_table = generate_meta_table(GetParam());
  expected_table->append(reference_meta_table_values(GetParam()->name()));
  EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
}

TEST_P(MultiCacheMetaTablesTest, DoesNotFailWithoutCache) {
  Hyrise::get().default_pqp_cache = nullptr;
  const auto expected_table = reference_meta_table(GetParam()->name());
  const auto meta_table = generate_meta_table(GetParam());
  EXPECT_EQ(meta_table->row_count(), 0);
  EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
}

}  // namespace opossum
