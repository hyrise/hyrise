#pragma once

#include <unordered_map>
#include <string>
#include <memory>
#include <vector>

namespace opossum {

class Chunk;
class Table;

class TpchDbGenerator final {
 public:
  TpchDbGenerator(float scale_factor, uint32_t chunk_size = 0);

  std::unordered_map<std::string, std::shared_ptr<Table>> generate();
  void generate_and_store();
  void generate_and_export(const std::string &path);

 private:
  uint32_t _chunk_size;

  std::shared_ptr<Table> _customer_table;

  /**
   * CUSTOMER current chunk population
   */
  std::vector<int64_t> _c_custkeys;
  std::vector<std::string> _c_names;
  std::vector<std::string> _c_addresses;
  std::vector<int64_t> _c_nation_codes;
  std::vector<std::string> _c_phones;
  std::vector<int64_t> _c_acctbals;
  std::vector<std::string> _c_mktsegments;
  std::vector<std::string> _c_comments;

  void _store_customer(const customer_t &customer);
  void _emit_customer_chunk();

  template<typename T>
  void _add_column_to_chunk(Chunk &chunk, std::vector<T> && data);
};

}