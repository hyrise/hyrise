#include "tpcc_table_generator.hpp"

#include <memory>
#include <string>
#include <utility>

#include "storage/storage_manager.hpp"
#include "storage/value_column.hpp"

namespace opossum {

TPCCTableGenerator::TPCCTableGenerator() : _random_generator(RandomGenerator()) {}

std::shared_ptr<Item> TPCCTableGenerator::generate_item(size_t id, bool is_original) {
  auto item = std::make_shared<Item>();
  item->i_id = id;
  item->i_im_id = _random_generator.number(1, 10000);
  item->i_name = _random_generator.astring(14, 24);
  item->i_price = _random_generator.number(100, 10000) / 100;
  auto dataString = _random_generator.astring(26, 50);
  if (is_original) {
    std::string originalString("ORIGINAL");
    size_t start_pos = _random_generator.number(0, dataString.length() - originalString.length());
    dataString.replace(start_pos, originalString.length(), originalString);
  }
  item->i_data = dataString;

  return item;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_items_table() {
  auto item_table = std::make_shared<Table>(_chunk_size);

  auto vector_size = _item_cardinality;

  // setup columns
  item_table->add_column("I_ID", "int", false);
  item_table->add_column("I_IM_ID", "int", false);
  item_table->add_column("I_NAME", "string", false);
  item_table->add_column("I_PRICE", "float", false);
  item_table->add_column("I_DATA", "string", false);

  tbb::concurrent_vector<int> i_id_column(vector_size);
  tbb::concurrent_vector<int> i_im_id_column(vector_size);
  tbb::concurrent_vector<std::string> i_name_column(vector_size);
  tbb::concurrent_vector<float> i_price_column(vector_size);
  tbb::concurrent_vector<std::string> i_data_column(vector_size);

  auto original_ids = _random_generator.select_unique_ids(_item_cardinality / 10, 1, _item_cardinality);

  auto chunk = Chunk();
  for (size_t i = 0; i < _item_cardinality; i++) {
    if (i % 10000 == 0) {
      std::cout << "inserting into items vectors at " << i << std::endl;
    }

    bool is_original = original_ids.find(i) != original_ids.end();
    auto item = generate_item(i, is_original);

    i_id_column[i] = item->i_id;
    i_im_id_column[i] = item->i_im_id;
    i_name_column[i] = item->i_name;
    i_price_column[i] = item->i_price;
    i_data_column[i] = item->i_data;
  }

  chunk.add_column(std::make_shared<ValueColumn<int>>(std::move(i_id_column)));
  chunk.add_column(std::make_shared<ValueColumn<int>>(std::move(i_im_id_column)));
  chunk.add_column(std::make_shared<ValueColumn<std::string>>(std::move(i_name_column)));
  chunk.add_column(std::make_shared<ValueColumn<float>>(std::move(i_price_column)));
  chunk.add_column(std::make_shared<ValueColumn<std::string>>(std::move(i_data_column)));

  item_table->add_chunk(std::move(chunk));

  return item_table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_warehouse_table() {
  auto warehouse_table = std::make_shared<Table>(_chunk_size);
  return warehouse_table;
}
}  // namespace opossum
