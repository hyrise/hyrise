#include <deque>

#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/progressive/chunk_sink.hpp"
#include "operators/progressive/proxy_table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"

namespace hyrise {



}  // namespace

using namespace hyrise;  // NOLINT(build/namespaces)
using namespace hyrise::expression_functional;  // NOLINT(build/namespaces)

int main() {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  constexpr auto scale_factor = 0.1f;
  auto benchmark_config = BenchmarkConfig::get_default_config();
  TPCHTableGenerator(scale_factor, ClusteringConfiguration::None,
                     std::make_shared<BenchmarkConfig>(benchmark_config))
      .generate_and_store();

  const auto& lineitem_table = Hyrise::get().storage_manager.get_table("lineitem");
  auto lineitem_wrapper = std::make_shared<TableWrapper>(lineitem_table);
  lineitem_wrapper->execute();

  auto initial_chunk_exchange = std::make_shared<ChunkSink>(lineitem_wrapper, SinkType::PipelineStart);
  auto scan_join_exchange = std::make_shared<ChunkSink>(nullptr, SinkType::Forwarding);

  //
  // Populate first chunk exchange.
  // TODO: check if we can rate limit here.
  //
  // for (auto chunk_id = ChunkID{0}; chunk_id < lineitem_chunk_count; ++chunk_id) {
  //   initial_chunk_exchange.add_chunk(lineitem_table->get_chunk(chunk_id));
  //   std::cerr << "Adding chunk " << chunk_id << '\n';
  // }
  // initial_chunk_exchange.set_finished();

  auto pipeline_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  pipeline_jobs.reserve(16);

  //
  // Table Scan #1
  //
  pipeline_jobs.emplace_back(std::make_shared<JobTask>([&]() {
    auto scan = ProxyTableScan(lineitem_wrapper, initial_chunk_exchange,
      scan_join_exchange, between_inclusive_(pqp_column_(ColumnID{6}, DataType::Float, false, ""), 0.0, 100));
    // auto proxy_table_scan = std::make_shared<ProxyTableScan>(lineitem_wrapper, initial_chunk_exchange,
    //   scan_join_exchange, between_inclusive_(pqp_column_(ColumnID{6}, DataType::Float, false, ""), 0.0, 100));
    // proxy_table_scan->execute();
  }));
  pipeline_jobs.back()->schedule();

  //
  // Table Scan #2
  //
  pipeline_jobs.emplace_back(std::make_shared<JobTask>([&]() {
    auto proxy_table_scan = std::make_shared<ProxyTableScan>(lineitem_wrapper, initial_chunk_exchange,
      scan_join_exchange, between_inclusive_(pqp_column_(ColumnID{6}, DataType::Float, false, ""), 0.0, 100));
    proxy_table_scan->execute();
  }));
  pipeline_jobs.back()->schedule();

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(pipeline_jobs);
  Hyrise::get().scheduler()->finish();

  return 0;
}
