#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/chunk.hpp"
#include "storage/value_segment.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "resolve_type.hpp"
#include "utils/timer.hpp"
#include "iostream"
#include "types.hpp"
#include "dummy_mvcc_delete.hpp"
#include "../benchmarklib/random_generator.hpp"

void setup();
void run_benchmark(double threshold, size_t updates);

using namespace opossum;  // NOLINT

int main() {
  setup();

  run_benchmark(0.5, 1'000'000);
  return 0;
}

void run_benchmark(const double threshold, const size_t updates) {
  auto& tm = TransactionManager::get();
  RandomGenerator rg;
  DummyMvccDelete mvcc_delete(threshold);

  auto column = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "number");

  Timer timer;

  for (size_t i=0; i < updates; i++) {
    const auto transaction_context = tm.new_transaction_context();
    const auto rand = static_cast<int>(rg.random_number(0,19'999));
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

    transaction_context->commit();

    if (i % 100 == 0) {
      const auto &table = StorageManager::get().get_table("mvcc_benchmark");
      std::cout << timer.lap().count() << std::endl;
      std::cout << "Chunks: " << table->chunk_count() <<
                " Rows: " << table->row_count() <<
                " CID: " << transaction_context->commit_id() <<
                " Valid: " << validate->get_output()->row_count() << std::endl;
    }

    if (i % 1000 == 0) {
        mvcc_delete.start();
    }
  }
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
