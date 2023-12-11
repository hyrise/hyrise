#include "data_loading_plugin.hpp"

#include <algorithm>
#include <iostream>
#include <random>
#include <unordered_map>

#include <boost/container_hash/hash.hpp>

#include "hyrise.hpp"
#include "operators/delete.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/place_holder_segment.hpp"
#include "storage/pos_lists/entire_chunk_pos_list.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

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

namespace {

void log(const uint32_t call_id, const std::string& log_message) {
  // std::cerr << std::format("#{}: {}\n", call_id, log_message) << std::flush;
}

}  // namespace

namespace hyrise {

std::string DataLoadingPlugin::description() const {
  return "Data Loading Plugin for TPC-H";
}

/**
 * Right now, we only support a single generated data set. Our idea of passing attributes to a table (e.g.,
 * `where _dbgen.SF=10`) needs to be intercepted before the SQL translation (at least here, the storage manager is accessed).
 * Ideas:
 *    - "Dynamic views" would be one idea but there are pretty early rewritten to actual queries. So probably no.
 *    - Intercept at the SQL translation and creation of a lazy table at this point might work.
 */
void DataLoadingPlugin::start() {
  // I am too tired. How can we have settings (registered at start) and read these settings at start?
  // Do we need to have the set up in a plugin-callable function?

  // auto& settings_manager = Hyrise::get().settings_manager;
  // const auto scale_factor_setting_name = std::string{"data_loading_scale_factor"};
  // auto scale_factor_setting = std::make_shared<DataLoadingSetting>("data_loading_scale_factor");
  // scale_factor_setting->register_at_settings_manager();
  // Assert(settings_manager.has_setting("data_loading_scale_factor"), "Please set setting '" + scale_factor_setting_name + "' before starting plugin.");
  // const auto scale_factor_setting = settings_manager.get_setting(scale_factor_setting_name);

  // This is currently the easier approach.
  const auto* env_scale_factor = std::getenv("SCALE_FACTOR");
  Assert(env_scale_factor, "Environment variable SCALE_FACTOR must be set.");
  _scale_factor = std::strtof(env_scale_factor, nullptr);
  // _scale_factor = 0.6f;

  auto tpch_table_generator = TPCHTableGenerator(_scale_factor, ClusteringConfiguration::None);
  tpch_table_generator.reset_and_initialize();

  std::cout << "Setting up place holder tables for TPC-H." << std::endl;

  for (const auto& [table_name, estimated_row_count] :
    std::initializer_list<std::tuple<std::string, size_t>>{{"lineitem", static_cast<size_t>(static_cast<double>(tpch_table_generator.orders_row_count() * 4.0) * 1.1)},
                                                           {"orders", tpch_table_generator.orders_row_count()},
                                                           {"customer", tpch_table_generator.customer_row_count()},
                                                           {"part", tpch_table_generator.part_row_count()},
                                                           {"partsupp", tpch_table_generator.part_row_count() * 4},
                                                           {"supplier", tpch_table_generator.supplier_row_count()},
                                                           {"nation", tpch_table_generator.nation_row_count()},
                                                           {"region", tpch_table_generator.region_row_count()}}) {
    const auto chunk_count = static_cast<size_t>(std::ceil(static_cast<double>(estimated_row_count) / static_cast<double>(Chunk::DEFAULT_SIZE)));

    auto table = tpch_table_generator.create_empty_table(table_name);
    auto table_statistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>{};

    auto place_holder_statistics = std::unordered_map<std::pair<std::shared_ptr<Table>, ColumnID>, std::shared_ptr<BaseAttributeStatistics>>{};
    
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      auto segments = Segments{};
      auto chunk_statistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>{};

      auto column_id = ColumnID{0};
      for (const auto& column_definition : table->column_definitions()) {
        
        resolve_data_type(column_definition.data_type, [&, table_name=table_name](auto data_type) {
          using ColumnDataType = typename decltype(data_type)::type;
          segments.emplace_back(std::make_shared<PlaceHolderSegment>(table, table_name, chunk_id, column_id, column_definition.nullable));

          auto attribute_statistics = std::shared_ptr<BaseAttributeStatistics>();
          if (place_holder_statistics.contains({table, column_id})) {
            attribute_statistics = place_holder_statistics[{table, column_id}];
          } else {
            auto new_attribute_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
            new_attribute_statistics->set_table_origin(table, table_name, column_id);
            place_holder_statistics.emplace(std::pair{table, column_id}, new_attribute_statistics);
            attribute_statistics = new_attribute_statistics;
          }
          chunk_statistics.emplace_back(attribute_statistics);
          if (chunk_id == 0) {
            table_statistics.emplace_back(attribute_statistics);
          }
        });
        ++column_id;
      }

      table->append_chunk(segments, std::make_shared<MvccData>(Chunk::DEFAULT_SIZE, CommitID{0}));
      table->get_chunk(chunk_id)->finalize();
      table->get_chunk(chunk_id)->set_pruning_statistics(chunk_statistics);
    }

    // Generate place holder statistics.
    table->set_table_statistics(std::make_shared<TableStatistics>(std::move(table_statistics), estimated_row_count));
    generate_chunk_pruning_statistics(table);

    auto& storage_manager = Hyrise::get().storage_manager;
    storage_manager.add_table(table_name, table);
    std::cout << "Added table " << table_name << " to the storage manager." << std::endl;
  }
}

void DataLoadingPlugin::stop() {}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> DataLoadingPlugin::provided_user_executable_functions() {
  return {{"LoadTableAndStatistics", [&]() {
    _load_table_and_statistics();
  }}};
}

void DataLoadingPlugin::_load_table_and_statistics() {
  auto& settings_manager = Hyrise::get().settings_manager;

  thread_local static auto call_id = std::atomic<uint32_t>{0};
  ++call_id;

  // We copy and shuffle the settings vector, hoping that not all callers are trying the create the same tables and
  // columns in the same order.
  auto settings_lock = std::unique_lock<std::mutex>{_settings_mutex};
  auto settings_names = settings_manager.setting_names();
  settings_lock.unlock();

  auto random_device = std::random_device{};
  auto generator = std::mt19937{random_device()};
  std::shuffle(settings_names.begin(), settings_names.end(), generator);

  auto& log_manager = Hyrise::get().log_manager;
  const auto settings_key = std::string{"dbgen_request__"};
  for (const auto& setting_name : settings_names) {
    auto table_name = std::string{};
    auto column_id = ColumnID{INVALID_COLUMN_ID};
    auto load_table = false;
    auto load_column = false;

    if (setting_name.starts_with(settings_key)) {
      const auto table_name_begin = settings_key.length();
      Assert(table_name_begin < setting_name.length(), "Unexpected setting key.");
      const auto column_id_begin = setting_name.find("::", table_name_begin);
      const auto column_id_end = setting_name.find("__", column_id_begin);
      Assert(column_id_begin < setting_name.length(), "Unexpected setting key.");
      table_name = setting_name.substr(table_name_begin, column_id_begin - table_name_begin);
      column_id = static_cast<ColumnID::base_type>(std::stoi(setting_name.substr(column_id_begin + 2, column_id_end - column_id_begin)));
      Assert(table_name != "" && column_id != INVALID_COLUMN_ID, "Settings not parsed correctly.");
    }

    if (table_name == "" && column_id == INVALID_COLUMN_ID) {
      continue;
    }

    {
      auto tables_columns_lock = std::scoped_lock{_tables_mutex, _columns_mutex};

      // Check if we need to load table. If so, mark entry in _tables.
      auto table_search = std::find_if(_tables.begin(), _tables.end(), [&](auto& entry) { return entry.first == table_name; });
      if (table_search == _tables.end()) {
        load_table = true;
        _tables.emplace_back(std::pair{table_name, std::string{"requested"}});
      }

      auto table_column_search = std::find_if(_columns.begin(), _columns.end(), [&](auto& entry) {
        return entry.first.first == table_name && entry.first.second == column_id;
      });
      if (table_column_search == _columns.end()) {
        load_column = true;
        _columns.emplace_back(std::pair{std::pair{table_name, column_id}, std::string{"requested"}});
      }
    }

    Assert(!load_table || load_column, "Cannot load table but no column.");

    if (load_table || load_column) {
      log(call_id.load(), std::format("Load requested: table '{}': {}\t-\tcolumn ID {}: {}", table_name, load_table, static_cast<size_t>(column_id), load_column));
    }

    if (!load_table && !load_column) {
      log(call_id.load(), std::format("No load request obtained: table '{}': {}\t-\tcolumn ID {}: {}", table_name, load_table, static_cast<size_t>(column_id), load_column)); 
    }

    if (load_table) {
      {
        auto dbgen_lock = std::lock_guard<std::mutex>{_dbgen_mutex}; 

        if (_table_cache.contains(table_name)) {
          log(call_id.load(), std::format("We have already created {} and can skip generation.", table_name));
        } else {
          auto tpch_table_generator = TPCHTableGenerator(_scale_factor, ClusteringConfiguration::None);
          tpch_table_generator.reset_and_initialize();

          log(call_id.load(), std::format("Generating {} table ...", table_name));
          auto timer = Timer{};
          if (table_name == "orders" || table_name == "lineitem") {
            const auto& [orders_table, lineitem_table] = tpch_table_generator.create_orders_and_lineitem_tables(tpch_table_generator.orders_row_count(), 0);
            _table_cache["orders"] = orders_table;
            _table_cache["lineitem"] = lineitem_table;
          } else if (table_name == "customer") {
            const auto& customer_table = tpch_table_generator.create_customer_table(tpch_table_generator.customer_row_count(), 0);
            _table_cache["customer"] = customer_table;
          } else if (table_name == "part" || table_name == "partsupp") {
            const auto& [part_table, partsupp_table] = tpch_table_generator.create_part_and_partsupp_tables(tpch_table_generator.part_row_count(), 0);
            _table_cache["part"] = part_table;
            _table_cache["partsupp"] = partsupp_table;
          } else if (table_name == "supplier") {
            const auto& supplier_table = tpch_table_generator.create_supplier_table(tpch_table_generator.supplier_row_count(), 0);
            _table_cache["supplier"] = supplier_table;
          } else if (table_name == "nation") {
            const auto& nation_table = tpch_table_generator.create_nation_table(tpch_table_generator.nation_row_count(), 0);
            _table_cache["nation"] = nation_table;
          } else if (table_name == "region") {
            const auto& region_table = tpch_table_generator.create_region_table(tpch_table_generator.region_row_count(), 0);
            _table_cache["region"] = region_table;
          } else {
            Fail("Table loading not implemented yet.");
          }
          log(call_id.load(), std::format("Generating {} table done ({}).", table_name, timer.lap_formatted()));
        }
      }

      auto place_holder_table = Hyrise::get().storage_manager.get_table(table_name);
      const auto chunk_count_place_holder_table = place_holder_table->chunk_count();
      const auto chunk_count_generated_table = _table_cache[table_name]->chunk_count();
      Assert(chunk_count_place_holder_table >= chunk_count_generated_table, "Place holder table was not sized sufficiently.");

      auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::References);

      if (chunk_count_place_holder_table != chunk_count_generated_table) {
        auto sstream = std::stringstream{};
        sstream << "WARNING: chunk count of '" << table_name << "' between place holder (" << chunk_count_place_holder_table;
        sstream << " chunks) and generated table (" << chunk_count_generated_table << " chunks) differs.\n";
        std::cout << sstream.str();

        for (auto chunk_id = ChunkID{chunk_count_generated_table}; chunk_id < chunk_count_place_holder_table; ++chunk_id) {
          // A lot of boiler plate for deletion, but a clean delete might be a benefit in the long run.
          const auto chunk_size = place_holder_table->get_chunk(chunk_id)->size();
          const auto entire_chunk_pos_list = std::make_shared<const EntireChunkPosList>(chunk_id, chunk_size);
          const auto reference_segment = std::make_shared<ReferenceSegment>(place_holder_table, ColumnID{0}, entire_chunk_pos_list);

          table->append_chunk(Segments{reference_segment});
        }

        const auto table_wrapper = std::make_shared<TableWrapper>(table);
        const auto delete_op = std::make_shared<Delete>(table_wrapper);
        const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
        delete_op->set_transaction_context(transaction_context);
        table_wrapper->execute();
        delete_op->execute();
        transaction_context->commit();

        for (auto chunk_id = ChunkID{chunk_count_generated_table}; chunk_id < chunk_count_place_holder_table; ++chunk_id) {
          place_holder_table->remove_chunk(chunk_id);
          Assert(!place_holder_table->get_chunk(chunk_id), "Chunk not correctly deleted.");
        }
      }

      const auto success_log_message = std::string{"dbgen_success__"} + table_name + "::";
      log_manager.add_message("", success_log_message);
      auto tables_lock = std::lock_guard<std::mutex>{_tables_mutex};
      _tables.emplace_back(std::pair{table_name, std::string{"generated"}});
    }

    // Encode and load table.
    if (load_column) {
      auto histogram_duration_string = std::string{};
      Assert(column_id != INVALID_COLUMN_ID, "Cannot load invalid column.");
      log(call_id.load(), std::format("Attempting to process column {}@{} ...", table_name, static_cast<size_t>(column_id)));

      // If we are here, tables has either been created (load_table==true) or it is currently being created.
      data_loading_utils::wait_for_table(std::string{"dbgen_success__"} + table_name + "::");
      log(call_id.load(), std::format("Processing column {}@{} ...", table_name, static_cast<size_t>(column_id)));

      Assert(_table_cache.contains(table_name), "Table '" + table_name + "' not yet created.");
      auto table = _table_cache[table_name];
      Assert(table->row_count() > 0, "Table '" + table_name + "' is still empty.");
      auto timer = Timer{};

      const auto chunk_count = table->chunk_count();
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        auto generated_chunk = table->get_chunk(chunk_id);
        Assert(generated_chunk, "Generated tables should never store invalid chunks.");
        auto stored_chunk = Hyrise::get().storage_manager.get_table(table_name)->get_chunk(chunk_id);
        stored_chunk->replace_segment(column_id, generated_chunk->get_segment(column_id));
      }

      const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, table->row_count() / 2'000));

      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs.reserve(1 + chunk_count);
      const auto column_data_type = table->column_data_type(column_id);
      jobs.emplace_back(std::make_shared<JobTask>([&]() {
        resolve_data_type(column_data_type, [&](auto type) {
          log(call_id.load(), std::format("Processing column {}@{}: creating histogram", table_name, static_cast<size_t>(column_id)));
          auto timer = Timer{};
          using ColumnDataType = typename decltype(type)::type;

          const auto output_column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();

          const auto histogram =
              EqualDistinctCountHistogram<ColumnDataType>::from_column(*table, column_id, histogram_bin_count);

          if (histogram) {
            output_column_statistics->set_statistics_object(histogram);

            // Use the insight that the histogram will only contain non-null values to generate the NullValueRatio
            // property
            const auto null_value_ratio =
                table->row_count() == 0
                    ? 0.0f
                    : 1.0f - (static_cast<float>(histogram->total_count()) / static_cast<float>(table->row_count()));
            output_column_statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(null_value_ratio));
          } else {
            // Failure to generate a histogram currently only stems from all-null segments.
            // TODO(anybody) this is a slippery assumption. But the alternative would be a full segment scan...
            output_column_statistics->set_statistics_object(std::make_shared<NullValueRatioStatistics>(1.0f));
          }

          {
            const auto lock_guard = std::lock_guard<std::mutex>{_histograms_mutex};
            auto updated_table_statistics = Hyrise::get().storage_manager.get_table(table_name)->table_statistics()->column_statistics;
            updated_table_statistics[column_id] = output_column_statistics;
            Hyrise::get().storage_manager.get_table(table_name)->set_table_statistics(std::make_shared<TableStatistics>(std::move(updated_table_statistics),
                                                                                                                      static_cast<float>(table->row_count())));
          }

          histogram_duration_string = timer.lap_formatted();
          log(call_id.load(), std::format("Processing column {}@{} done: created histogram", table_name, static_cast<size_t>(column_id)));
        });
      }));

      auto pruning_statistics_ns = std::atomic<uint64_t>{0};
      auto encoding_ns = std::atomic<uint64_t>{0};
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
          // std::cerr << std::format("Processing column {}@{}: processing chunk {} of {}.\n",
          //                          table_name, static_cast<size_t>(column_id),
          //                          static_cast<size_t>(chunk_id), static_cast<size_t>(chunk_count));
          const auto chunk = table->get_chunk(chunk_id);
          const auto segment = chunk->get_segment(column_id);
          auto timer = Timer{};

          // We don't parallelize encoding and chunk statistics, as the statistics can profit from dictionary encoding.
          const auto encoded_segment = ChunkEncoder::encode_segment(segment, table->column_data_type(column_id), SegmentEncodingSpec{EncodingType::Dictionary});
          chunk->replace_segment(column_id, encoded_segment);
          encoding_ns += timer.lap().count();
          
          resolve_data_and_segment_type(*(chunk->get_segment(column_id)), [&](auto type, auto& typed_segment) {
            using SegmentType = std::decay_t<decltype(typed_segment)>;
            using ColumnDataType = typename decltype(type)::type;

            const auto segment_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();

            if constexpr (std::is_same_v<SegmentType, DictionarySegment<ColumnDataType>>) {
              // we can use the fact that dictionary segments have an accessor for the dictionary
              const auto& dictionary = *typed_segment.dictionary();
              create_pruning_statistics_for_segment(*segment_statistics, dictionary);
            } else {
              // if we have a generic segment we create the dictionary ourselves
              auto iterable = create_iterable_from_segment<ColumnDataType>(typed_segment);
              auto values = std::unordered_set<ColumnDataType>{};
              iterable.for_each([&](const auto& value) {
                // we are only interested in non-null values
                if (!value.is_null()) {
                  values.insert(value.value());
                }
              });
              pmr_vector<ColumnDataType> dictionary{values.cbegin(), values.cend()};
              std::sort(dictionary.begin(), dictionary.end());
              create_pruning_statistics_for_segment(*segment_statistics, dictionary);
            }

            const auto lock_guard = std::lock_guard<std::mutex>{_chunk_statistics_mutex};
            auto updated_chunk_statistics = Hyrise::get().storage_manager.get_table(table_name)->get_chunk(chunk_id)->pruning_statistics();
            (*updated_chunk_statistics)[column_id] = segment_statistics;
            Hyrise::get().storage_manager.get_table(table_name)->get_chunk(chunk_id)->set_pruning_statistics(updated_chunk_statistics);
          });
          pruning_statistics_ns += timer.lap().count();
          // std::cerr << std::format("Processing column {}@{} done: processed chunk {} of {}.\n",
          //                          table_name, static_cast<size_t>(column_id),
          //                          static_cast<size_t>(chunk_id), static_cast<size_t>(chunk_count));
        }));
      }
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

      // auto sstream = std::stringstream{};
      // sstream << "Histogram and Compression: we scheduled tasks IDs: ";
      // for (const auto& job : jobs) {
      //   sstream << job->id() << ", ";
      // }
      // sstream << "\n";
      // std::cerr << sstream.str();

      log(call_id.load(), std::format("Processing column {}@{} done ({} [histogram: {}, encoding: {}, chunk stats: {}]).",
                                      table_name, static_cast<size_t>(column_id), timer.lap_formatted(), histogram_duration_string,
                                      format_duration(std::chrono::nanoseconds{encoding_ns}),
                                      format_duration(std::chrono::nanoseconds{pruning_statistics_ns})));

      Hyrise::get().default_lqp_cache->clear();
      Hyrise::get().default_pqp_cache->clear();

      const auto success_log_message = std::string{"dbgen_success__"} + table_name + "::" + std::to_string(column_id) + "__";
      log_manager.add_message("", success_log_message);
    }
  }
}

EXPORT_PLUGIN(DataLoadingPlugin);

}  // namespace hyrise
