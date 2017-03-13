#pragma once

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "abstract_operator.hpp"
#include "product.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

namespace opossum {

class SortMergeJoin : public AbstractOperator {
 public:
  SortMergeJoin(const std::shared_ptr<AbstractOperator> left, const std::shared_ptr<AbstractOperator> right,
                optional<std::pair<const std::string&, const std::string&>> column_names, const std::string& op,
                const JoinMode mode);

  void execute() override;
  std::shared_ptr<const Table> get_output() const override;
  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 private:
  std::shared_ptr<Table> sort_left_table(ColumnVisitable& impl);
  std::shared_ptr<Table> sort_right_table(ColumnVisitable& impl);
  void sort_merge_join(std::shared_ptr<Table> table_left, std::shared_ptr<Table> table_right);

  struct SortContext : ColumnVisitableContext {
    SortContext(ChunkID chunk_id, bool left) : _chunk_id{chunk_id}, _write_to_sorted_left_table{left} {}

    ChunkID _chunk_id;
    bool _write_to_sorted_left_table;
  };

  template <typename T>
  class SortMergeJoinImpl : public AbstractOperatorImpl, public ColumnVisitable {
   protected:
    size_t _partition_count = 8;

   public:
    SortMergeJoinImpl<T>(SortMergeJoin& sort_merge_join);

    // AbstractOperatorImpl implementation
    void execute() override;
    std::shared_ptr<Table> get_output() const override;

    // struct used for materialized sorted Chunk
    struct SortedChunk {
      SortedChunk() {}
      // std::vector<T> _values;
      // std::shared_ptr<PosList> _original_positions;
      std::vector<std::pair<T, RowID>> _values;
      std::map<uint64_t, uint32_t> _histogram;
      std::map<uint64_t, uint32_t> _prefix;
    };

    // struct used for materialized sorted Table
    struct SortedTable {
      SortedTable() {}
      std::vector<SortedChunk> _partition;
      std::map<uint64_t, uint32_t> _histogram;
    };

    // Sort functions
    void sort_table(std::shared_ptr<SortedTable> sort_table, std::shared_ptr<const Table> input,
                    const std::string& column_name, bool left);
    void sort_partition(const std::vector<ChunkID> chunk_ids, std::shared_ptr<const Table> input,
                        const std::string& column_name, bool left);
    // Partitioning in case of Non-Equi-Join
    void value_based_table_partitioning(std::shared_ptr<SortedTable> sort_table, uint64_t min, uint64_t max);
    void value_based_partitioning();

    // helper functions to turn T2 into bits (uint)
    template <typename T2>
    typename std::enable_if<std::is_arithmetic<T2>::value, size_t>::type get_bits(T2 value) {
      auto result = reinterpret_cast<size_t*>(&value);
      return *result;
    }
    template <typename T2>
    typename std::enable_if<!std::is_arithmetic<T2>::value, size_t>::type get_bits(T2 value) {
      auto result = reinterpret_cast<const size_t*>(value.c_str());
      return *result;
    }

    template <typename T2>
    typename std::enable_if<std::is_arithmetic<T2>::value, size_t>::type get_radix(T2 value, size_t radix_bits) {
      auto result = reinterpret_cast<size_t*>(&value);
      return *result & radix_bits;
    }
    template <typename T2>
    typename std::enable_if<!std::is_arithmetic<T2>::value, size_t>::type get_radix(T2 value, size_t radix_bits) {
      auto result = reinterpret_cast<const size_t*>(value.c_str());
      return *result & radix_bits;
    }
    // Looks for matches and possibly calls helper function to add match to _sort_merge_join._output
    void partition_join(uint32_t partition_number, std::vector<PosList>& pos_lists_left,
                        std::vector<PosList>& pos_lists_right);
    void perform_join();
    // builds output based on pos_list_left/-_right
    void build_output();

    std::shared_ptr<PosList> dereference_pos_list(std::shared_ptr<const Table> input_table, size_t column_id,
                                                  std::shared_ptr<const PosList> pos_list);

    // ColumnVisitable implementation
    void handle_value_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override;
    void handle_dictionary_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override;
    void handle_reference_column(ReferenceColumn& column, std::shared_ptr<ColumnVisitableContext> context) override;

   private:
    SortMergeJoin& _sort_merge_join;
    std::shared_ptr<SortedTable> _sorted_left_table;
    std::shared_ptr<SortedTable> _sorted_right_table;
    std::function<bool(const T&, const T&)> _compare;
    SortedTable _left_table;
    SortedTable _right_table;
  };

  std::unique_ptr<AbstractOperatorImpl> _impl;
  std::shared_ptr<Product> _product;
  std::string _left_column_name;
  std::string _right_column_name;
  std::string _op;
  JoinMode _mode;

  std::shared_ptr<PosList> _pos_list_left;
  std::shared_ptr<PosList> _pos_list_right;
  std::shared_ptr<Table> _output;
};
}  // namespace opossum
