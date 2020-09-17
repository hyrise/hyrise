#include "clustering_plugin.hpp"

#include "operators/get_table.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "resolve_type.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"

namespace opossum {

std::string ClusteringPlugin::description() const { return "ClusteringPlugin"; }

void ClusteringPlugin::start() {
  Hyrise::get().log_manager.add_message(description(), "Initialized!", LogLevel::Info);
  _loop_thread = std::make_unique<PausableLoopThread>(THREAD_INTERVAL, [&](size_t) { _optimize_clustering(); });
}

void ClusteringPlugin::_optimize_clustering() {
  if (_optimized) return;

  _optimized = true;

  std::map<std::string, std::string> sort_orders = {{"orders_tpch_1", "o_orderkey"},
                                                    {"lineitem_tpch_1", "l_orderkey"}};
  auto threads = std::vector<std::thread>{};
  threads.reserve(sort_orders.size());

  for (const auto& sort_order : sort_orders) {
    const auto table_name_outer = sort_order.first;
    const auto column_name_outer = sort_order.second;
    threads.emplace_back([&, table_name_outer, column_name_outer] {
      const auto table_name = table_name_outer;
      const auto column_name = column_name_outer;

      if (!Hyrise::get().storage_manager.has_table(table_name)) {
        Hyrise::get().log_manager.add_message(
            description(), "No optimization possible with given parameters for " + table_name + " table!",
            LogLevel::Debug);
        return;
      }
      auto table = Hyrise::get().storage_manager.get_table(table_name);

      const auto sort_column_id = table->column_id_by_name(column_name);

      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->execute();
      auto sort = Sort{table_wrapper,
                       {SortColumnDefinition{sort_column_id, SortMode::Ascending}},
                       Chunk::DEFAULT_SIZE,
                       Sort::ForceMaterialization::Yes};
      sort.execute();
      const auto immutable_sorted_table = sort.get_output();

      Assert(immutable_sorted_table->chunk_count() == table->chunk_count(), "Mismatching chunk_count");

      table = std::make_shared<Table>(immutable_sorted_table->column_definitions(), TableType::Data,
                                      table->target_chunk_size(), UseMvcc::Yes);
      const auto column_count = immutable_sorted_table->column_count();
      const auto chunk_count = immutable_sorted_table->chunk_count();
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = immutable_sorted_table->get_chunk(chunk_id);
        auto mvcc_data = std::make_shared<MvccData>(chunk->size(), CommitID{0});
        Segments segments{};
        for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
          const auto abstract_segment = chunk->get_segment(column_id);
          std::shared_ptr<AbstractSegment> new_segment;
          const auto data_type = table->column_data_type(column_id);
          new_segment =
              ChunkEncoder::encode_segment(abstract_segment, data_type, SegmentEncodingSpec{EncodingType::Dictionary});
          segments.emplace_back(new_segment);
        }
        table->append_chunk(segments, mvcc_data);
        auto sort_column_definition = SortColumnDefinition(ColumnID{sort_column_id}, SortMode::Ascending);
        table->get_chunk(chunk_id)->finalize();
        table->get_chunk(chunk_id)->set_individually_sorted_by(sort_column_definition);
      }

      table->set_table_statistics(TableStatistics::from_table(*table));
      generate_chunk_pruning_statistics(table);

      Hyrise::get().storage_manager.replace_table(table_name, table);
      if (Hyrise::get().default_lqp_cache) Hyrise::get().default_lqp_cache->clear();
      if (Hyrise::get().default_pqp_cache) Hyrise::get().default_pqp_cache->clear();

      Hyrise::get().log_manager.add_message(
          description(), "Applied new clustering configuration (" + column_name + ") to " + table_name + " table.",
          LogLevel::Warning);
    });
  }

  for (auto& thread : threads) thread.join();
}

void ClusteringPlugin::stop() {}

EXPORT_PLUGIN(ClusteringPlugin)

}  // namespace opossum
