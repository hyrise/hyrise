#pragma once

#include <map>
#include <memory>
#include <string>
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
    size_t _partition_count = 1;

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
      std::map<T, uint32_t> _chunk_index;
      std::map<T, uint32_t> _histogram;
      std::map<T, uint32_t> _prefix;
    };

    // struct used for materialized sorted Table
    struct SortedTable {
      SortedTable() {}
      std::vector<SortedChunk> _chunks;
      std::map<T, uint32_t> _histogram;
      std::map<T, uint64_t> _prefix;
    };

    // Sort functions
    void sort_left_table();
    void sort_left_chunks(ChunkID chunk_id);
    void sort_right_table();
    void sort_right_chunks(ChunkID chunk_id);
    // Looks for matches and possibly calls helper function to add match to _sort_merge_join._output
    void perform_join();
    // builds output based on pos_list_left/-_right
    void build_output();

    // ColumnVisitable implementation
    virtual void handle_value_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context);
    virtual void handle_dictionary_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context);
    virtual void handle_reference_column(ReferenceColumn& column, std::shared_ptr<ColumnVisitableContext> context);

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
