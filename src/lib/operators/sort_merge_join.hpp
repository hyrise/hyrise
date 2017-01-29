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

  struct JoinContext : ColumnVisitableContext {
    JoinContext(std::shared_ptr<BaseColumn> column_left, std::shared_ptr<BaseColumn> column_right,
                ChunkID left_chunk_id, ChunkID right_chunk_id, JoinMode mode)
        : _column_left{column_left},
          _column_right{column_right},
          _left_chunk_id{left_chunk_id},
          _right_chunk_id{right_chunk_id},
          _mode{mode} {};

    std::shared_ptr<BaseColumn> _column_left;
    std::shared_ptr<BaseColumn> _column_right;
    ChunkID _left_chunk_id;
    ChunkID _right_chunk_id;
    JoinMode _mode;
  };

  template <typename T>
  class SortMergeJoinImpl : public AbstractOperatorImpl, public ColumnVisitable {
   public:
    SortMergeJoinImpl<T>(SortMergeJoin& nested_loop_join);

    // AbstractOperatorImpl implementation
    void execute() override;
    std::shared_ptr<Table> get_output() const override;

    // struct used for materialized sorted Chunk
    class SortedChunk {
     public:
      SortedChunk() {}
      std::vector<T> _values;
      std::shared_ptr<PosList> _original_positions;
      std::map<T, int> _chunk_index;
      std::map<T, int> _histogram;
      std::map<T, int> _prefix;
    };

    // struct used for materialized sorted Table
    class SortedTable {
     public:
      SortedTable() {}
      std::vector<SortedChunk> _chunks;
      std::map<T, int> _histogram;
      std::map<T, int> _prefix;
    };

    // Context for sorting tables
    struct SortContext : ColumnVisitableContext {
      SortContext() {}

      std::map<T, int> _histogram;
      std::map<T, int> _prefix;
    };

    // Sort functions
    SortedTable sort_left_table();
    SortedTable sort_right_table();
    void perform_join(SortedTable& left, SortedTable& right);

    // ColumnVisitable implementation
    virtual void handle_value_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context);
    virtual void handle_dictionary_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context);
    virtual void handle_reference_column(ReferenceColumn& column, std::shared_ptr<ColumnVisitableContext> context);

    void join_value_value(ValueColumn<T>& left, ValueColumn<T>& right, std::shared_ptr<JoinContext> context,
                          bool reverse_order = false);
    void join_value_dictionary(ValueColumn<T>& left, DictionaryColumn<T>& right, std::shared_ptr<JoinContext> context,
                               bool reverse_order = false);
    void join_value_reference(ValueColumn<T>& left, ReferenceColumn& right, std::shared_ptr<JoinContext> context,
                              bool reverse_order = false);
    void join_dictionary_dictionary(DictionaryColumn<T>& left, DictionaryColumn<T>& right,
                                    std::shared_ptr<JoinContext> context, bool reverse_order = false);
    void join_dictionary_reference(DictionaryColumn<T>& left, ReferenceColumn& right,
                                   std::shared_ptr<JoinContext> context, bool reverse_order = false);
    void join_reference_reference(ReferenceColumn& left, ReferenceColumn& right, std::shared_ptr<JoinContext> context,
                                  bool reverse_order = false);

   private:
    SortMergeJoin& _nested_loop_join;
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
