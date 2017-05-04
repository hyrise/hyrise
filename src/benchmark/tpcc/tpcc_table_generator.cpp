#include "tpcc_table_generator.hpp"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "storage/storage_manager.hpp"
#include "storage/value_column.hpp"

namespace opossum {

TPCCTableGenerator::TPCCTableGenerator() : _random_generator(RandomGenerator()) {}

template<typename T>
void fill_column(tbb::concurrent_vector<T> &v, const std::function<T(size_t)> &fn) {
  for (size_t i = 0; i < v.size(); i++) {
    v[i] = fn(i);
  }
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

  fill_column(i_id_column, [](int i){return i});
  fill_column(i_im_id_column, [](int i){_random_generator.number(1, 10000)});
  fill_column(i_name_column, [](int i){_random_generator.astring(14, 24)});
  fill_column(i_price_column, [](int i){_random_generator.number(100, 10000) / 100});
  auto original_ids = _random_generator.select_unique_ids(_item_cardinality / 10, 1, _item_cardinality);
  fill_column(i_data_column, [&original_ids](int i){
    std::string data = _random_generator.astring(26, 50);
    bool is_original = original_ids.find(i) != original_ids.end();
    if (is_original) {
      std::string originalString("ORIGINAL");
      size_t start_pos = _random_generator.number(0, dataString.length() - originalString.length());
      data.replace(start_pos, originalString.length(), originalString);
    }
    return data
  });

  auto chunk = Chunk();
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
