#include <iostream>
#include <memory>

#include "types.hpp"
#include "storage/column_accessor.hpp"
#include "storage/value_column.hpp"

using namespace opossum;  // NOLINT

int main() {
  auto value_column = std::make_shared<ValueColumn<int>>();
  value_column->append({5});
  auto accessor = get_column_accessor<int>(value_column);
  auto val = accessor->access(ChunkOffset{0});
  std::cout << "val: " << *val << std::endl;
  return 0;
}
