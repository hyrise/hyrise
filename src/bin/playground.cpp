#include <deque>

#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/print.hpp"
#include "operators/progressive/chunk_sink.hpp"
#include "operators/progressive/progressive_map.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/timer.hpp"

namespace hyrise {



}  // namespace

using namespace hyrise;  // NOLINT(build/namespaces)
using namespace hyrise::expression_functional;  // NOLINT(build/namespaces)

int main() {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  constexpr auto SCALE_FACTOR = 10.0f;
  auto benchmark_config = BenchmarkConfig::get_default_config();
  TPCHTableGenerator(SCALE_FACTOR, ClusteringConfiguration::None,
                     std::make_shared<BenchmarkConfig>(benchmark_config))
      .generate_and_store();

  auto runs = size_t{0};
  while (runs < 1) {
  // while (runs < 1'000) {
    ++runs;
    auto timer_query_start = Timer{};

    const auto& lineitem_table = Hyrise::get().storage_manager.get_table("lineitem");
    auto lineitem_wrapper = std::make_shared<TableWrapper>(lineitem_table);
    lineitem_wrapper->execute();

    auto initial_chunk_exchange = std::make_shared<ChunkSink>(lineitem_wrapper, SinkType::PipelineStart);
    initial_chunk_exchange->set_name("Initial_Sink");
    auto scan1_scan2_exchange = std::make_shared<ChunkSink>(lineitem_wrapper, SinkType::Forwarding);  // input op is actually only needed for initial sink, but currently interface needs "some random" operator.
    scan1_scan2_exchange->set_name("Scan1_Scan2_Sink");
    auto scan2_scan3_exchange = std::make_shared<ChunkSink>(lineitem_wrapper, SinkType::Forwarding);
    scan2_scan3_exchange->set_name("Scan2_Scan3_Sink");
    auto scan3_projection_exchange = std::make_shared<ChunkSink>(lineitem_wrapper, SinkType::Forwarding);
    scan3_projection_exchange->set_name("Scan3_Projection_Sink");
    auto final_exchange = std::make_shared<ChunkSink>(lineitem_wrapper, SinkType::PipelineEnd);
    final_exchange->set_name("Final_Sink");

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

    // TODO(Martin): see if we could make the "spawn functions resumable". Or rather: do we even need the Proxy Jobs? Why
    // not just listen to all sinks and issue tasks?

    //
    // Table Scan #1
    //
    pipeline_jobs.emplace_back(std::make_shared<JobTask>([&]() {
      auto map_op = std::make_shared<ProgressiveMap>(lineitem_wrapper, initial_chunk_exchange, scan1_scan2_exchange, OperatorType::TableScan);
      map_op->set_table_scan_predicate(between_upper_exclusive_(pqp_column_(ColumnID{10}, DataType::String, false, ""), "1993-01-01", "1994-01-01"));
      map_op->execute();
    }));
    pipeline_jobs.back()->schedule();  // As of now, we need to start this job immediately, otherwise we cannot "register"
                                       // the scan as a consumer of the initial_chunk_exchange as it is already executed.

    initial_chunk_exchange->execute();

    //
    // Table Scan #2
    //
    pipeline_jobs.emplace_back(std::make_shared<JobTask>([&]() {
      auto map_op = std::make_shared<ProgressiveMap>(lineitem_wrapper, scan1_scan2_exchange, scan2_scan3_exchange, OperatorType::TableScan);
      map_op->set_table_scan_predicate(between_inclusive_(pqp_column_(ColumnID{6}, DataType::Float, false, ""), 0.03, 0.05001));
      map_op->execute();
    }));
    pipeline_jobs.back()->schedule();

    //
    // Table Scan #3
    //
    pipeline_jobs.emplace_back(std::make_shared<JobTask>([&]() {
      auto map_op = std::make_shared<ProgressiveMap>(lineitem_wrapper, scan2_scan3_exchange, scan3_projection_exchange, OperatorType::TableScan);
      map_op->set_table_scan_predicate(less_than_(pqp_column_(ColumnID{4}, DataType::Float, false, ""), 24.0));
      map_op->execute();
    }));
    pipeline_jobs.back()->schedule();

    //
    // Projection #4
    //
    pipeline_jobs.emplace_back(std::make_shared<JobTask>([&]() {
      auto map_op = std::make_shared<ProgressiveMap>(lineitem_wrapper, scan3_projection_exchange, final_exchange, OperatorType::Projection);
      map_op->set_projection_expressions();
      map_op->execute();
    }));
    pipeline_jobs.back()->schedule();

    Assert(pipeline_jobs.size() < std::thread::hardware_concurrency(),
           "Pipeline too large, long-running map operator instances might block processing tasks.");

    Hyrise::get().scheduler()->wait_for_tasks(pipeline_jobs);

    final_exchange->execute();  // Just gathering the result (copying vector of shared_ptr's).
    std::cerr << "Query runtime: " << timer_query_start.lap_formatted() << '\n';
    std::cerr << "Result has " << final_exchange->get_output()->row_count() << " rows.\n";
  }

  // Print::print(final_exchange->get_output());

  Hyrise::get().scheduler()->finish();

  return 0;
}
