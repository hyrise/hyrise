#include <iostream>
#include "operators/print.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"

#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  auto table = std::make_shared<opossum::Table>(10);
  table->create_range_partitioning(opossum::ColumnID{1}, {100000000, 1000000000});
  table->add_column("country", opossum::DataType::String);
  table->add_column("population", opossum::DataType::Int, true);
  table->append({"China", 1388720000});
  table->append({"India", 1326720000});
  table->append({"United States", 326474000});
  table->append({"Germany", 82521653});

  auto table_wrapper = std::make_shared<opossum::TableWrapper>(table);
  table_wrapper->execute();

  auto print_partitioned_operator = std::make_shared<opossum::Print>(table_wrapper, std::cout);
  std::cout << "### Partitioned table" << std::endl;
  print_partitioned_operator->execute();
  std::cout << std::endl << std::endl;

  auto sort_operator = std::make_shared<opossum::Sort>(table_wrapper, opossum::ColumnID{0});
  sort_operator->execute();

  auto print_operator = std::make_shared<opossum::Print>(sort_operator, std::cout);
  std::cout << "### Sorted table" << std::endl;
  print_operator->execute();

  return 0;
}
