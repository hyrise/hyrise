#include "../benchmarklib/random_generator.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "fstream"
#include "iostream"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/timer.hpp"

#ifdef __APPLE__
#define DYNAMIC_LIBRARY_SUFFIX ".dylib"
#elif __linux__
#define DYNAMIC_LIBRARY_SUFFIX ".so"
#endif

void setup();
void run_benchmark(bool use_plugin, size_t updates, size_t interval, std::string filename);

using namespace opossum;  // NOLINT

int main() {
  setup();
  run_benchmark(true, 200'000, 1000, "benchmark1.csv");

  StorageManager::get().drop_table("mvcc_benchmark");

  setup();
  run_benchmark(false, 200'000, 1000, "benchmark2.csv");
  return 0;
}

void run_benchmark(const bool use_plugin, const size_t updates, const size_t interval, const std::string filename) {
  if (use_plugin) {
    auto& pm = PluginManager::get();
    pm.load_plugin(std::string(TEST_PLUGIN_DIR) + "libMvccDeletePlugin" + std::string(DYNAMIC_LIBRARY_SUFFIX));
  }

  const auto tbl = StorageManager::get().get_table("mvcc_benchmark");
  auto column = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "number");

  auto& tm = TransactionManager::get();

  RandomGenerator rg;

  std::ofstream file;
  file.open(filename);
  file << "num_tx,num_rows,time\n";

  Timer timer;

  for (size_t i = 0; i < updates; i++) {
    if (i % interval == 0) {
      auto time = timer.lap().count();
      std::cout << "Tx: " << i << " Rows: " << tbl->row_count() << " Time: " << time
                << " Chunks: " << tbl->chunk_count() << std::endl;
      file << i << "," << tbl->row_count() << "," << time << "\n";
    }

    const auto transaction_context = tm.new_transaction_context();
    const auto rand = static_cast<int>(rg.random_number(0, 19'999));
    const auto expr = expression_functional::equals_(column, rand);

    const auto gt = std::make_shared<GetTable>("mvcc_benchmark");
    gt->set_transaction_context(transaction_context);

    const auto validate = std::make_shared<Validate>(gt);
    validate->set_transaction_context(transaction_context);

    const auto where = std::make_shared<TableScan>(validate, expr);
    where->set_transaction_context(transaction_context);

    const auto update = std::make_shared<Update>("mvcc_benchmark", where, where);
    update->set_transaction_context(transaction_context);

    gt->execute();
    validate->execute();
    where->execute();
    update->execute();

    if (!update->execute_failed()) {
      transaction_context->commit();
    } else {
      transaction_context->rollback();
      i--;
    }
  }
  file.close();
  if (use_plugin) PluginManager::get().unload_plugin("MvccDeletePlugin");
}

void setup() {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("number", data_type_from_type<int>());

  std::vector<int> vec;
  vec.reserve(20'000);
  for (int i = 0; i < 20'000; i++) {
    vec.emplace_back(i);
  }

  Segments segments;
  const auto value_segment = std::make_shared<ValueSegment<int>>(std::move(vec));
  segments.emplace_back(value_segment);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 20'000, UseMvcc::Yes);
  table->append_chunk(segments);

  auto& sm = StorageManager::get();
  sm.add_table("mvcc_benchmark", table);
}
