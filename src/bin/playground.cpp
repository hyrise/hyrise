#include <iostream>
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"

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
  auto print_operator = std::make_shared<opossum::Print>(table_wrapper, std::cout);

  table_wrapper->execute();
  print_operator->execute();

  return 0;
}
