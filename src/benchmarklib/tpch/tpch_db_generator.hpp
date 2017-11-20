#pragma once

#include <unordered_map>
#include <string>
#include <memory>
#include <vector>

#include "boost/hana.hpp"
#include "storage/chunk.hpp"
#include "storage/value_column.hpp"

namespace opossum {

class Chunk;
class Table;

template<typename ... Args>
class ChunkBuilder {
 public:
  void append_row(Args &&... args) {
    _append_row<0>(std::forward<Args>(args)...);
  }

  Chunk emit_chunk() {
    Chunk chunk;

    hana::for_each([&] (auto vector) {
      chunk.add_column(std::make_shared<ValueColumn<decltype(vector)>>(vector));
    }, _column_vectors);

    return chunk;
  }

  size_t row_count() const {
    return std::get<0>(_column_vectors).size();
  }

 private:
  boost::hana::tuple<std::vector<Args>...> _column_vectors;

  template<size_t index, typename Head, typename ... Tail>
  void _append_row(Head head, Tail ... tail) {
    std::get<index>(_column_vectors).emplace_back(head);
    _append_row<index + 1, Tail...>(tail...);
  }

  template<size_t index, typename Head>
  void _append_row(Head head) {
    std::get<index>(_column_vectors).emplace_back(head);
  }
};

class TpchDbGenerator final {
 public:
  TpchDbGenerator(float scale_factor, uint32_t chunk_size = 0);

  std::unordered_map<std::string, std::shared_ptr<Table>> generate();
//  void generate_and_store();
//  void generate_and_export(const std::string &path);

 private:
  uint32_t _chunk_size;
//
//  std::shared_ptr<Table> _customer_table;
//
//  /**
//   * CUSTOMER current chunk population
//   */
//  std::vector<int64_t> _c_custkeys;
//  std::vector<std::string> _c_names;
//  std::vector<std::string> _c_addresses;
//  std::vector<int64_t> _c_nation_codes;
//  std::vector<std::string> _c_phones;
//  std::vector<int64_t> _c_acctbals;
//  std::vector<std::string> _c_mktsegments;
//  std::vector<std::string> _c_comments;

//  void _store_customer(const customer_t &customer);
//  void _emit_customer_chunk();
//
//  template<typename T>
//  void _add_column_to_chunk(Chunk &chunk, std::vector<T> && data);
};

}