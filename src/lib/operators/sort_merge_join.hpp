#pragma once

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "product.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

namespace opossum {

class SortMergeJoin : public AbstractJoinOperator {
 public:
  SortMergeJoin(const std::shared_ptr<const AbstractOperator> left, const std::shared_ptr<const AbstractOperator> right,
                optional<std::pair<std::string, std::string>> column_names, const std::string& op, const JoinMode mode,
                const std::string& prefix_left, const std::string& prefix_right);

  std::shared_ptr<const Table> on_execute() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  struct SortContext : ColumnVisitableContext {
    SortContext(ChunkID chunk_id, bool left) : chunk_id(chunk_id), write_to_sorted_left_table(left) {}

    ChunkID chunk_id;
    bool write_to_sorted_left_table;
  };

  template <typename T>
  class SortMergeJoinImpl : public AbstractJoinOperatorImpl, public ColumnVisitable {
   protected:
    // should be 2^x
    size_t _partition_count = 1;

   public:
    SortMergeJoinImpl<T>(SortMergeJoin& sort_merge_join);

    // AbstractJoinOperatorImpl implementation
    std::shared_ptr<const Table> on_execute() override;

    // struct used for materialized sorted Chunk
    struct SortedChunk {
      SortedChunk() {}

      std::vector<std::pair<T, RowID>> values;
      std::map<uint64_t, uint32_t> histogram;
      std::map<uint64_t, uint32_t> prefix;

      std::map<T, uint32_t> histogram_v;
      std::map<T, uint32_t> prefix_v;
    };

    // struct used for materialized sorted Table
    struct SortedTable {
      SortedTable() {}

      std::vector<SortedChunk> partition;
      std::map<uint64_t, uint32_t> histogram;

      std::map<T, uint32_t> histogram_v;
    };

    // Sort functions
    void sort_table(std::shared_ptr<SortedTable> sort_table, std::shared_ptr<const Table> input,
                    const std::string& column_name, bool left);
    void sort_partition(const std::vector<ChunkID> chunk_ids, std::shared_ptr<const Table> input,
                        const std::string& column_name, bool left);

    // Partitioning in case of Non-Equi-Join
    void value_based_table_partitioning(std::shared_ptr<SortedTable> sort_table, std::vector<T>& p_values);
    void value_based_partitioning();

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

    void addSmallerValues(uint32_t partition_number, std::shared_ptr<SortedTable>& table_smaller_values,
                          std::vector<PosList>& pos_list_smaller, std::vector<PosList>& pos_list_greater,
                          uint32_t max_index_smaller_values, RowID greaterId);

    void addGreaterValues(uint32_t partition_number,
                          std::shared_ptr<SortMergeJoinImpl::SortedTable>& table_greater_values,
                          std::vector<PosList>& pos_list_smaller, std::vector<PosList>& pos_list_greater,
                          uint32_t max_index_greater_values, RowID smallerId);

    void perform_join();

    // builds output based on pos_list_left/-_right
    void build_output(std::shared_ptr<Table>& output);

    std::shared_ptr<PosList> dereference_pos_list(std::shared_ptr<const Table> input_table, size_t column_id,
                                                  std::shared_ptr<const PosList> pos_list);

    // ColumnVisitable implementation
    void handle_value_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override;
    void handle_dictionary_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context) override;
    void handle_reference_column(ReferenceColumn& column, std::shared_ptr<ColumnVisitableContext> context) override;

   protected:
    SortMergeJoin& _sort_merge_join;
    std::shared_ptr<SortedTable> _sorted_left_table;
    std::shared_ptr<SortedTable> _sorted_right_table;
    SortedTable _left_table;
    SortedTable _right_table;
  };

  std::unique_ptr<AbstractJoinOperatorImpl> _impl;
  std::string _left_column_name;
  std::string _right_column_name;

  // std::shared_ptr<Table> _output;
  std::shared_ptr<PosList> _pos_list_left;
  std::shared_ptr<PosList> _pos_list_right;
};
}  // namespace opossum
