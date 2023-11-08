#include "data_loading_plugin.hpp"

#include <unordered_map>

#include <boost/container_hash/hash.hpp>

#include "hyrise.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk.hpp"
#include "storage/place_holder_segment.hpp"
#include "tpch/tpch_table_generator.hpp"

namespace std {

template <>
struct hash<std::pair<std::shared_ptr<hyrise::Table>, hyrise::ColumnID>> {
  size_t operator()(const std::pair<std::shared_ptr<hyrise::Table>, hyrise::ColumnID>& statistics_key) const {
  auto hash = size_t{0};
  boost::hash_combine(hash, statistics_key.first);
  boost::hash_combine(hash, statistics_key.second);
  return hash;
  }
};

}  // namespace std

namespace hyrise {

std::string DataLoadingPlugin::description() const {
  return "Data Loading Plugin for TPC-H";
}

/**
 * Right now, we only support a single generated data set. Our idea of passing attributes to a table (e.g.,
 * `where _dbgen.SF=10`) needs to be intercepted before the SQL translation (at least here the storage manager is accessed).
 * Ideas:
 	* "Dynamic views" would be one idea but there are pretty early rewritten to actual queries. So probably no.
 	* Intercept at the SQL translation and creation of a lazy table at this point might work.
 */
void DataLoadingPlugin::start() {
  // TODO: read configuration parametes which are later required. They state the generator and which tables to create.

  constexpr auto SCALE_FACTOR = 10.0;

  auto tpch_table_generator = TPCHTableGenerator(SCALE_FACTOR, ClusteringConfiguration::None);
  tpch_table_generator.reset_and_initialize();

  const auto upper_lineitem_row_count = tpch_table_generator.orders_row_count() * 4;
  const auto upper_lineitem_chunk_count = static_cast<size_t>(std::ceil(static_cast<double>(upper_lineitem_row_count) * 1.1 / static_cast<double>(Chunk::DEFAULT_SIZE)));

  auto lineitem_table = tpch_table_generator.create_empty_table("lineitem");
  auto table_statistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>{};

  auto place_holder_statistics = std::unordered_map<std::pair<std::shared_ptr<Table>, ColumnID>, std::shared_ptr<BaseAttributeStatistics>>{};
  
  for (auto chunk_id = ChunkID{0}; chunk_id < upper_lineitem_chunk_count; ++chunk_id) {
    auto segments = Segments{};
    auto chunk_statistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>{};

    auto column_id = ColumnID{0};
    for (const auto& column_definition : lineitem_table->column_definitions()) {
      
      resolve_data_type(column_definition.data_type, [&](auto data_type) {
        using ColumnDataType = typename decltype(data_type)::type;
        segments.emplace_back(std::make_shared<PlaceHolderSegment>(lineitem_table, column_id, column_definition.nullable));

        auto attribute_statistics = std::shared_ptr<BaseAttributeStatistics>();
        if (place_holder_statistics.contains({lineitem_table, column_id})) {
          attribute_statistics = place_holder_statistics[{lineitem_table, column_id}];
        } else {
          attribute_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
          attribute_statistics->set_table_origin(lineitem_table, column_id);
          place_holder_statistics.emplace(std::pair{lineitem_table, column_id}, attribute_statistics);
        }
        chunk_statistics.emplace_back(attribute_statistics);
        if (chunk_id == 0) {
          table_statistics.emplace_back(attribute_statistics);
        }
      });
      ++column_id;
    }

    lineitem_table->append_chunk(segments, std::make_shared<MvccData>(Chunk::DEFAULT_SIZE, CommitID{0}));
    lineitem_table->get_chunk(chunk_id)->finalize();
    lineitem_table->get_chunk(chunk_id)->set_pruning_statistics(chunk_statistics);
  }

  // First generate mock stats ...
  lineitem_table->set_table_statistics(std::make_shared<TableStatistics>(std::move(table_statistics), upper_lineitem_row_count));
  generate_chunk_pruning_statistics(lineitem_table);

  auto& storage_manager = Hyrise::get().storage_manager;
  storage_manager.add_table("lineitem", lineitem_table);
}

void DataLoadingPlugin::stop() {}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> DataLoadingPlugin::provided_user_executable_functions() {
  return {{"LoadTableAndStatistics", [&]() {
    _load_table_and_statistics();
  }}};
}

void DataLoadingPlugin::_load_table_and_statistics() {
  // auto& settings_manager = Hyrise::get().settings_manager;

  // auto table_name = std::string{};
  // auto column_id = ColumnID{INVALID_COLUMN_ID};
  // auto table_loaded = false;
  // auto column_loaded = false;

  // { 
  //   auto settings_lock = std::unique_lock<std::mutex>{_settings_mutex};

  //   const auto settings_key = "dbgen_request__";
  //   for (const auto& setting_name : settings_manager.setting_names()) {
  //     if (setting_name.starts_with(settings_key)) {
  //       const auto table_name_begin = settings_key.length();
  //       Assert(table_name_begin < setting_name.length(), "Unexpected setting key.");
  //       const auto column_name_begin = setting_name.find("::", table_name_begin);
  //       Assert(column_name_begin < setting_name.length(), "Unexpected setting key.");
  //       table_name = setting_name.substr(table_name_begin, column_name_begin - table_name_begin);
  //       column_id = static_cast<ColumnID::base_type>(std::stoi(setting_name.substr(column_name_begin)));
  //       Assert(table_name != "" && column_id != INVALID_COLUMN_ID, "Settings not parsed correctly.")
  //     }

      
  //     for (const auto& [loaded_table_name, loaded_column_id] : _loaded_columns) {
  //       if (loaded_table_name = table_name) {
  //         table_loaded = true;
  //       }

  //       if (loaded_column_id == column_id) {
  //         column_loaded = true;
  //       }
  //     }

  //     if (!table_loaded) {
  //       auto lock = std::lock_guard<std::mutex>{_dbgen_mutex};

  //       auto tpch_table_generator = TPCHTableGenerator(SCALE_FACTOR, ClusteringConfiguration::None);
  //       tpch_table_generator.reset_and_initialize();

  //       if (table_name == "orders" || table_name == "lineitem") {
  //         const auto& [orders_table, lineitem_table] = tpch_table_generator.create_orders_and_lineitem_tables(tpch_table_generator.orders_row_count(), 0);
  //       } else if (table_name == "customer") {
  //         const auto& customer_table = tpch_table_generator.create_customer_table(tpch_table_generator.customer_row_count(), 0);
  //       }
  //     }
  //   }
  // }
}

EXPORT_PLUGIN(DataLoadingPlugin);

}  // namespace hyrise
