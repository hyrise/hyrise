#pragma once

#include <memory>
#include <string>
#include "storage/table.hpp"

namespace opossum {

struct Item {
  size_t i_id;
  size_t i_im_id;
  std::string i_name;
  float i_price;
  std::string i_data;
};

class TPCCTableGenerator {
 public:
  TPCCTableGenerator();

  virtual ~TPCCTableGenerator() = default;

  std::shared_ptr<Item> generate_item(size_t id, bool is_original);
  std::shared_ptr<Table> generate_items_table();

 protected:
  const size_t _chunk_size = 100;
  const size_t _item_cardinality = 100000;
};
}  // namespace opossum
