#include <memory>

#include "benchmark/benchmark.h"

#include "all_type_variant.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/progressive_utils.hpp"
#include "utils/timer.hpp"

namespace {

using namespace hyrise;

std::shared_ptr<Table> get_ny_taxi_data_table(std::string&& path) {
  auto csv_meta = CsvMeta{.config = ParseConfig{.null_handling = NullHandling::NullStringAsValue, .rfc_mode = false},
                          .columns = {{"vendor_name", "string", true},
                                      {"Trip_Pickup_DateTime", "string", true},
                                      {"Trip_Dropoff_DateTime", "string", true},
                                      {"Passenger_Count", "int", true},
                                      {"Trip_Distance", "float", true},
                                      {"Start_Lon", "float", true},
                                      {"Start_Lat", "float", true},
                                      {"Rate_Code", "string", true},
                                      {"store_and_forward", "string", true},
                                      {"End_Lon", "float", true},
                                      {"End_Lat", "float", true},
                                      {"Payment_Type", "string", true},
                                      {"Fare_Amt", "float", true},
                                      {"surcharge", "float", true},
                                      {"mta_tax", "float", true},
                                      {"Tip_Amt", "float", true},
                                      {"Tolls_Amt", "float", true},
                                      {"Total_Amt", "float", true}}};

  // // We are working a bit around the CSV interface of Hyrise here to have some parallelization.
  // auto table_column_definitions = TableColumnDefinitions{};
  // for (const auto& column : csv_meta.columns) {
  //   table_column_definitions.emplace_back(column.name, data_type_to_string.right.at(column.type), column.nullable);
  // }

  // auto table = std::make_shared<Table>(table_column_definitions, TableType::Data, Chunk::DEFAULT_SIZE);

  auto dir_iter = std::filesystem::directory_iterator("resources/progressive/" + path);
  const auto file_count =
      static_cast<size_t>(std::distance(std::filesystem::begin(dir_iter), std::filesystem::end(dir_iter)));
  // We could write chunks directly to a merged table by each CSV job, but we'll collect them first to ensure the
  // "correct order".
  auto tables = std::vector<std::shared_ptr<Table>>(file_count);
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(file_count);

  auto position = size_t{0};
  for (const auto& file : std::filesystem::directory_iterator("resources/progressive/" + path)) {
    jobs.emplace_back(std::make_shared<JobTask>([&, file]() {
      auto load_timer = Timer{};
      auto loaded_table = CsvParser::parse(file.path().string(), Chunk::DEFAULT_SIZE, csv_meta);
      std::cerr << std::format("Loaded CSV {} with {} rows in {}.\n", file.path().filename().string(),
                               loaded_table->row_count(), load_timer.lap_formatted());

      tables[position] = loaded_table;
      ++position;
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  auto& final_table = tables[0];
  for (auto table_id = size_t{1}; table_id < file_count; ++table_id) {
    for (auto chunk_id = ChunkID{0}; chunk_id < tables[table_id]->chunk_count(); ++chunk_id) {
      final_table->append_chunk(progressive::get_chunk_segments(tables[table_id]->get_chunk(chunk_id)),
                                tables[table_id]->get_chunk(chunk_id)->mvcc_data());
      final_table->last_chunk()->set_immutable();
    }
  }

  auto timer = Timer{};
  ChunkEncoder::encode_all_chunks(final_table, SegmentEncodingSpec{EncodingType::Dictionary});
  std::cerr << "Encoded table with " << final_table->row_count() << " rows in " << timer.lap_formatted() << ".\n";

  return final_table;
}

}  // namespace

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

void benchmark_tablescan_impl(benchmark::State& state, const std::shared_ptr<const AbstractOperator> in,
                              ColumnID left_column_id, const PredicateCondition predicate_condition,
                              const AllParameterVariant right_parameter) {
  const auto left_operand = pqp_column_(left_column_id, in->get_output()->column_data_type(left_column_id),
                                        in->get_output()->column_is_nullable(left_column_id), "");
  auto right_operand = std::shared_ptr<AbstractExpression>{};
  if (right_parameter.type() == typeid(ColumnID)) {
    const auto right_column_id = boost::get<ColumnID>(right_parameter);
    right_operand = pqp_column_(right_column_id, in->get_output()->column_data_type(right_column_id),
                                in->get_output()->column_is_nullable(right_column_id), "");

  } else {
    right_operand = value_(boost::get<AllTypeVariant>(right_parameter));
  }

  const auto predicate = std::make_shared<BinaryPredicateExpression>(predicate_condition, left_operand, right_operand);

  auto warm_up = std::make_shared<TableScan>(in, predicate);
  warm_up->execute();
  for (auto _ : state) {
    auto table_scan = std::make_shared<TableScan>(in, predicate);
    table_scan->execute();
  }
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_TableScanConstant)(benchmark::State& state) {
  _clear_cache();
  benchmark_tablescan_impl(state, _table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 7);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_TableScanVariable)(benchmark::State& state) {
  _clear_cache();
  benchmark_tablescan_impl(state, _table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, ColumnID{1});
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_TableScanConstant_OnDict)(benchmark::State& state) {
  _clear_cache();
  benchmark_tablescan_impl(state, _table_dict_wrapper, ColumnID{0}, PredicateCondition::GreaterThanEquals, 7);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_TableScanVariable_OnDict)(benchmark::State& state) {
  _clear_cache();
  benchmark_tablescan_impl(state, _table_dict_wrapper, ColumnID{0}, PredicateCondition::GreaterThanEquals, ColumnID{1});
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_TableScan_Like)(benchmark::State& state) {
  const auto lineitem_table = load_table("resources/test_data/tbl/tpch/sf-0.001/lineitem.tbl");

  const auto lineitem_wrapper = std::make_shared<TableWrapper>(lineitem_table);
  lineitem_wrapper->never_clear_output();
  lineitem_wrapper->execute();

  const auto column_names_and_patterns = std::vector<std::pair<std::string, pmr_string>>({
      {"l_comment", pmr_string{"%final%"}},
      {"l_comment", pmr_string{"%final%requests%"}},
      {"l_shipinstruct", pmr_string{"quickly%"}},
      {"l_comment", pmr_string{"%foxes"}},
      {"l_comment", pmr_string{"%quick_y__above%even%"}},
  });

  for (auto _ : state) {
    for (const auto& column_name_and_pattern : column_names_and_patterns) {
      const auto column_id = lineitem_table->column_id_by_name(column_name_and_pattern.first);
      const auto column = pqp_column_(column_id, DataType::String, false, "");
      const auto predicate = like_(column, value_(column_name_and_pattern.second));

      auto table_scan = std::make_shared<TableScan>(lineitem_wrapper, predicate);
      table_scan->execute();
    }
  }
}

/**
 * This benchmark is a stub for some measurements. In the long run, we should think about a proper benchmark executable
 * like `hyriseBenchmarkProgressive` that does "things".
 * For now, we can work with that here. We use the GoogleBench framework but I kind of misuse it here (no typical
 * `for (auto _ : state)` loop to measure).
 */
BENCHMARK_F(MicroBenchmarkBasicFixture, BM_ProgressiveTableScan)(benchmark::State& state) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  std::cerr << Hyrise::get().topology << std::endl;

  const auto table = get_ny_taxi_data_table("full");
  // const auto table = get_ny_taxi_data_table("1000");
  const auto chunk_count = table->chunk_count();

  // Print::print(table);

  const auto column_id = table->column_id_by_name("Trip_Pickup_DateTime");
  const auto column = pqp_column_(column_id, DataType::String, true, "");
  const auto predicate = between_inclusive_(column, value_("2009-02-17 00:00:00"), value_("2009-02-23 23:59:59"));

  auto result_counts_and_timings =
      std::vector<std::pair<size_t, std::chrono::time_point<std::chrono::system_clock>>>(chunk_count);

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

  // This is the relevant part for the "ensamble scan". Below is the "default" scan in Hyrise. All chunks are
  // processed in order as concurrently processsable tasks (we just look at the finish times of each chunk to simulate
  // a "traditional" and a "progressive" (pipeling) table scan).
  // I guess, you would reformulate the loop below to process "some" chunks and then decide with which chunks to
  // continue.
  const auto start = std::chrono::system_clock::now();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      // We construct an intermediate table that only holds a single chunk as the table scan expects a table as the input.
      auto single_chunk_vector = std::vector{progressive::recreate_non_const_chunk(table->get_chunk(chunk_id))};

      auto single_chunk_table =
          std::make_shared<Table>(table->column_definitions(), TableType::Data, std::move(single_chunk_vector));
      auto table_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
      table_wrapper->execute();

      auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
      table_scan->execute();

      result_counts_and_timings[chunk_id] = {table_scan->get_output()->row_count(), std::chrono::system_clock::now()};
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // This is more or less for fun, right now. Above, we do the "traditional" table scan in Hyrise, but manually and
  // pretend that we would immediately push "ready" results to the next pipeline operator.
  // The "traditional" costs below assume that we yield all tuples at the end at once.
  // The cost model is `for each tuple: costs += runtime_to_yield_tuple`.
  auto max_runtime = start;
  auto costs_traditional_scan = double{0.0};
  auto costs_progressive_scan = double{0.0};
  for (const auto& [result_count, runtime] : result_counts_and_timings) {
    // std::cerr << "Chunk results >> " << result_count << '\n';
    max_runtime = std::max(max_runtime, runtime);
    costs_traditional_scan += static_cast<double>(result_count);
    costs_progressive_scan +=
        static_cast<double>(result_count) * std::chrono::duration<double, std::micro>{runtime - start}.count();
    // std::cerr << result_count << " & " << std::chrono::duration<double, std::micro>{runtime - start}.count() << " - max: " << max_runtime << '\n';
  }
  std::cerr << std::format(
      "costs_traditional_scan: {:10.2f}\n",
      costs_traditional_scan * std::chrono::duration<double, std::micro>{max_runtime - start}.count());
  std::cerr << std::format("costs_progressive_scan: {:10}\n", costs_progressive_scan);
  std::cerr << "Traditional scan took " << std::chrono::duration<double, std::micro>{max_runtime - start}.count()
            << " us.\n";
}

}  // namespace hyrise
