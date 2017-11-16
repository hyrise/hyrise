#include <iostream>

#include "operators/export_binary.hpp"
#include "storage/table.hpp"
#include "operators/table_wrapper.hpp"


int main() {
  auto expected_table = std::make_shared<opossum::Table>();
  expected_table->add_column("a", "int", true);
  expected_table->add_column("b", "float", true);
  expected_table->add_column("c", "long", true);
  expected_table->add_column("d", "string", true);
  expected_table->add_column("e", "double", true);

  expected_table->append({opossum::NULL_VALUE, 1.1f, 100, "one", 1.11});
  expected_table->append({2, opossum::NULL_VALUE, 200, "two", 2.22});
  expected_table->append({3, 3.3f, opossum::NULL_VALUE, "three", 3.33});
  expected_table->append({4, 4.4f, 400, opossum::NULL_VALUE, 4.44});
  expected_table->append({5, 5.5f, 500, "five", opossum::NULL_VALUE});

  auto table_wrapper = std::make_shared<opossum::TableWrapper>(std::move(expected_table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, "src/test/binary/AllTypesNullValues_new.bin");
  ex->execute();
}
