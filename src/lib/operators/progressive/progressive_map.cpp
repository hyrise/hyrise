#include "progressive_map.hpp"

#include <deque>
#include <memory>
#include <thread>
#include <vector>

#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/print.hpp"
#include "operators/progressive/chunk_sink.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/task_queue.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "utils/progressive_utils.hpp"
#include "utils/timer.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

ProgressiveMap::ProgressiveMap(const std::shared_ptr<const AbstractOperator>& input_operator,
                               std::shared_ptr<ChunkSink>& input_chunk_sink,
                               std::shared_ptr<ChunkSink>& output_chunk_sink, const OperatorType operator_type)
    : AbstractReadOnlyOperator(OperatorType::TableScan, input_operator, nullptr),
      _input_chunk_sink{input_chunk_sink},
      _output_chunk_sink{output_chunk_sink},
      _operator_type{operator_type} {
  Assert(_input_chunk_sink && _output_chunk_sink, "ProgressiveMap requires sinks at both ends.");
  Assert(_operator_type == OperatorType::TableScan || _operator_type == OperatorType::Projection,
         "Map is not supported for Operator '" + std::string{magic_enum::enum_name(_operator_type)} + "'.");
}

const std::string& ProgressiveMap::name() const {
  static const auto name = std::string{"ProgressiveMap"};
  return name;
}

void ProgressiveMap::set_table_scan_predicate(std::shared_ptr<AbstractExpression> predicate) {
  Assert(_operator_type == OperatorType::TableScan, "Not possible.");
  _table_scan_predicate = predicate;
}

void ProgressiveMap::set_projection_expressions(
    /* const std::vector<std::shared_ptr<AbstractExpression>>& expressions */) {
  Assert(_operator_type == OperatorType::Projection, "Not possible.");
}

std::shared_ptr<AbstractOperator> ProgressiveMap::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  auto copy =
      std::make_shared<ProgressiveMap>(copied_left_input, _input_chunk_sink, _output_chunk_sink, _operator_type);
  if (_operator_type == OperatorType::TableScan) {
    copy->set_table_scan_predicate(_table_scan_predicate);
  }
  return copy;
}

void ProgressiveMap::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> ProgressiveMap::_on_execute() {
  auto timer = Timer{};
  [[maybe_unused]] auto chunk_output_durations = std::deque<std::pair<std::chrono::nanoseconds, size_t>>{};
  const auto& input_table = left_input_table();

  const auto thread_count = std::thread::hardware_concurrency();
  [[maybe_unused]] const auto num_high_priority_tasks = thread_count * 1;

  auto statistics_mutex = std::mutex{};

  auto sstream = std::stringstream{};
  if (_operator_type == OperatorType::TableScan) {
    sstream << "(predicate: " << *_table_scan_predicate << ")";
  }
  // std::cerr << std::format("ProgressiveMap for operator {} started {}.\n",
  //                          std::string{magic_enum::enum_name(_operator_type)}, sstream.str());

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  [[maybe_unused]] auto job_id = uint32_t{0};
  auto debug_chunks_processed = size_t{0};
  auto unsuccessful_pulls = uint32_t{1};

  const auto node_queue_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler());
  Assert(node_queue_scheduler, "NodeQueueScheduler not set?");
  Assert(node_queue_scheduler->queues().size() == 1, "Unexpected NUMA topology.");
  while (!_input_chunk_sink->finished()) {
    const auto chunk = _input_chunk_sink->pull_chunk();
    if (!chunk) {
      if (!_input_chunk_sink->finished()) {
        unsuccessful_pulls *= 2;
        // const auto sleep_ms = std::chrono::milliseconds{std::min(uint32_t{3'000}, unsuccessful_pulls / 1000)};
        const auto sleep_ms = std::chrono::milliseconds{unsuccessful_pulls / 1000};
        std::this_thread::sleep_for(sleep_ms);
        if (unsuccessful_pulls > 1'000'000) {
          std::cerr << "Error: Returning dummy empty due to timing out.\n";
          _output_chunk_sink->set_all_chunks_added();
          return Table::create_dummy_table(input_table->column_definitions());
        }
        if (unsuccessful_pulls > 100'000) {
          std::cerr << std::format(
              "ProgressiveMap for operator {} {} sleeps for {} ms. Input sink has {} chunks and status of finished is: "
              "{} (queue length is {}).\n",
              std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), sleep_ms,
              _input_chunk_sink->chunk_count(), _input_chunk_sink->finished(),
              node_queue_scheduler->queues()[0]->estimate_load());
        }
        continue;
      }

      // All chunks processed. We're done.
      break;
    }
    unsuccessful_pulls = 1;

    // std::cerr << std::format("{}\n", std::string{magic_enum::enum_name(_operator_type)});

    jobs.emplace_back(std::make_shared<JobTask>(
        [&, /*job_id,*/ chunk]() {
          // std::cerr << std::format("ProgressiveMap for operator {} {} (id {}) spawned.\n",
          //                          std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), job_id);
          auto single_chunk_vector = std::vector<std::shared_ptr<Chunk>>{};
          single_chunk_vector.emplace_back(progressive::recreate_non_const_chunk(chunk));
          Assert(single_chunk_vector.size() == 1 && single_chunk_vector[0], "Unexpected removed chunk?");

          // TODO: place in `if constexpr (HYRISEDBUG)`.
          const auto first_is_reference_segment =
              std::dynamic_pointer_cast<ReferenceSegment>(single_chunk_vector[0]->get_segment(ColumnID{0})) != nullptr;
          for (auto column_id = ColumnID{0}; column_id < single_chunk_vector[0]->column_count(); ++column_id) {
            const auto is_reference_segment =
                std::dynamic_pointer_cast<ReferenceSegment>(single_chunk_vector[0]->get_segment(column_id)) != nullptr;
            Assert(first_is_reference_segment == is_reference_segment, "Unexpected mismatch.");
          }

          auto single_chunk_table = std::make_shared<Table>(
              input_table->column_definitions(), first_is_reference_segment ? TableType::References : TableType::Data,
              std::move(single_chunk_vector), first_is_reference_segment ? UseMvcc::No : UseMvcc::Yes);
          auto lineitem_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
          lineitem_wrapper->never_clear_output();
          lineitem_wrapper->execute();

          ++debug_chunks_processed;
          auto op_output = std::shared_ptr<const Table>{};
          if (_operator_type == OperatorType::TableScan) {
            auto table_scan = std::make_shared<TableScan>(lineitem_wrapper, _table_scan_predicate);
            table_scan->set_output_sink(
                _output_chunk_sink);  // Not used yet. The idea is to later emit tuples very early
                                      // even within single-chunk scans.
            table_scan->never_clear_output();
            // std::cerr << std::format("ProgressiveMap for operator {} {} (id {}) executes scan.\n",
            //                        std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), job_id);
            table_scan->execute();
            // std::cerr << std::format("ProgressiveMap for operator {} {} (id {}) executed scan.\n",
            //                        std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), job_id);

            // auto ss = std::stringstream{};
            // ss << *_table_scan_predicate;
            // std::cerr << std::format("Processed chunk {} (predicate: {}).\n", debug_chunks_processed, ss.str());

            op_output = table_scan->get_output();
          } else if (_operator_type == OperatorType::Projection) {
            // We currently hard-code the expressions. Haven't found a much better way for now. We might need to access the
            // LQP node of the plan and do a similar approach as the SQL translator.
            const auto col_l_extendedprice =
                PQPColumnExpression::from_table(*lineitem_wrapper->get_output(), "l_extendedprice");
            const auto col_l_discount = PQPColumnExpression::from_table(*lineitem_wrapper->get_output(), "l_discount");
            auto projection = std::make_shared<Projection>(
                lineitem_wrapper, expression_vector(mul_(col_l_extendedprice, col_l_discount)));
            projection->never_clear_output();
            projection->execute();
            op_output = projection->get_output();
          } else {
            Fail("Unsupported operator.");
          }

          // std::cerr << std::format("ProgressiveMap for operator {} {} (id {}) adds result.\n",
          //                          std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), job_id);
          _output_chunk_sink->add_chunk(op_output);
          // std::cerr << std::format("ProgressiveMap for operator {} {} (id {}) added result.\n",
          //                          std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), job_id);

          // std::cerr << std::format("ProgressiveMap for operator {} {} (id {}) is done.\n",
          //                          std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), job_id);

          // auto lock_guard = std::lock_guard<std::mutex>{statistics_mutex};
          // chunk_output_durations.emplace_back(timer.lap(), op_output->get_chunk(ChunkID{0})->size());
        },
        // We schedule a fixed number of jobs with a high priority to ensure than early results are pipelined to other jobs
        // the first operators does not thrash the task queue.
        // TODO: align this "fixed number" with partitioning scheme later on. High for first partition, then default, high
        // again if coordinator states so.
        // (job_id < num_high_priority_tasks || job_id % 10 == 0) ? SchedulePriority::High : SchedulePriority::Default));
        SchedulePriority::Default));
    jobs.back()->schedule();
    // std::cerr << std::format("ProgressiveMap for operator {} {} scheduled job #{}.\n",
    //                          std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), job_id);
    Assert(jobs.back()->state() >= TaskState::Enqueued,
           "Unexpected task state of " + std::string{magic_enum::enum_name(jobs.back()->state())});
    ++job_id;
  }
  // std::cerr << std::format("ProgressiveMap for operator {} {} is waiting on all jobs.\n",
  //                          std::string{magic_enum::enum_name(_operator_type)}, sstream.str());
  // std::this_thread::sleep_for(std::chrono::milliseconds{500});
  // job_id = 0;
  // auto stop = false;
  // for (const auto& job : jobs) {
  //   if (job->state() != TaskState::Done) {
  //    std::cerr << std::format("#1 ProgressiveMap for operator {} {}: Job #{} not done, but {}.", std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), job_id, std::string{magic_enum::enum_name(job->state())});
  //    while (job->state() != TaskState::Done) {
  //      std::this_thread::sleep_for(std::chrono::milliseconds{1'000});
  //      std::cerr << std::format("#2 ProgressiveMap for operator {} {}: Waiting for Job #{}, current status {}.", std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), job_id, std::string{magic_enum::enum_name(job->state())});
  //    }
  //    // stop = true;
  //   }
  //   ++job_id;
  // }
  // if (stop) Fail("HMPF");

  Hyrise::get().scheduler()->wait_for_tasks(jobs);
  Assert(_input_chunk_sink->_chunks_added == _input_chunk_sink->_chunks_pulled, "error #1");
  Assert(_input_chunk_sink->_chunks_pulled == _output_chunk_sink->_chunks_added, "error #2");

  Assert(!_input_chunk_sink->pull_chunk(), "error #3: Able to pull a job even though sink is finished.");

  _output_chunk_sink->set_all_chunks_added();
  // std::cerr << std::format("ProgressiveMap for operator {} {} set `all_chunks_added`. Added {} chunks.\n",
  // std::string{magic_enum::enum_name(_operator_type)}, sstream.str(), _output_chunk_sink->chunk_count());

  // auto statistics_stream = std::stringstream{};
  // statistics_stream << std::format("ProgressiveMap for operator {} is done {}.\n",
  //                                  std::string{magic_enum::enum_name(_operator_type)}, sstream.str());
  // statistics_stream << "Duration (ns):\t\t\tCount:\n";
  // auto duration_sum = size_t{0};
  // auto output_rows_sum = size_t{0};
  // for (const auto& [duration_ns, row_count] : chunk_output_durations) {
  //   duration_sum += duration_ns.count();
  //   output_rows_sum += row_count;
  //   statistics_stream << std::format("\"{}{}\"", std::string{magic_enum::enum_name(_operator_type)}, sstream.str());
  //   statistics_stream << "," << duration_sum << "," << output_rows_sum << '\n';
  // }
  // std::cerr << statistics_stream.str();

  /*
  const auto chunk_count = input_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    
    const auto chunk = input_table->get_chunk(chunk_id);
    single_chunk_vector.emplace_back(recreate_non_const_chunk(chunk));
    Assert(single_chunk_vector.size() == 1 && single_chunk_vector[0], "Unexpected removed chunk?");

    auto single_chunk_table = std::make_shared<Table>(input_table->column_definitions(),
                                                      TableType::Data, std::move(single_chunk_vector), UseMvcc::Yes);
    auto lineitem_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
    lineitem_wrapper->execute();

    auto table_scan = std::make_shared<TableScan>(lineitem_wrapper, _table_scan_predicate);
    table_scan->set_output_sink(_output_chunk_sink);  // Not used yet. The idea is to later emit tuples very early even
                                                      // within single-chunk scans.
    table_scan->execute();
    std::cerr << "Executed chunk scan.\n";

    auto table_scan_output = table_scan->get_output();
    Assert(table_scan_output->chunk_count() < 2, "Unexpected chunk count.");
    output->append_chunk(get_chunk_segments(table_scan_output->get_chunk(ChunkID{0})));
  }
  */

  return Table::create_dummy_table(input_table->column_definitions());
}

}  // namespace hyrise
