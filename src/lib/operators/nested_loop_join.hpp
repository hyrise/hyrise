#pragma once

#include <iostream>
#include <memory>
#include <string>

#include "abstract_operator.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

namespace opossum {

class NestedLoopJoin : public AbstractOperator {
 public:
  NestedLoopJoin(std::shared_ptr<AbstractOperator> left, std::shared_ptr<AbstractOperator> right,
                 std::string& left_column_name, std::string& right_column_name, std::string& op);

  void execute() override;
  std::shared_ptr<const Table> get_output() const override;
  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 private:
  struct JoinContext : ColumnVisitableContext {
    JoinContext(std::shared_ptr<BaseColumn> column_left, std::shared_ptr<BaseColumn> column_right,
                ChunkID left_chunk_id, ChunkID right_chunk_id)
        : _column_left{column_left},
          _column_right{column_right},
          _left_chunk_id{left_chunk_id},
          _right_chunk_id{right_chunk_id} {};

    std::shared_ptr<BaseColumn> _column_left;
    std::shared_ptr<BaseColumn> _column_right;
    ChunkID _left_chunk_id;
    ChunkID _right_chunk_id;
  };

  template <typename T>
  class NestedLoopJoinImpl : public AbstractOperatorImpl, public ColumnVisitable {
   public:
    NestedLoopJoinImpl<T>(NestedLoopJoin& nested_loop_join);

    // AbstractOperatorImpl implementation
    virtual void execute();
    virtual std::shared_ptr<Table> get_output() const;

    // ColumnVisitable implementation
    virtual void handle_value_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context);
    virtual void handle_dictionary_column(BaseColumn& column, std::shared_ptr<ColumnVisitableContext> context);
    virtual void handle_reference_column(ReferenceColumn& column, std::shared_ptr<ColumnVisitableContext> context);

    void join_value_value(ValueColumn<T>& left, ValueColumn<T>& right, std::shared_ptr<JoinContext> context);
    void join_value_dictionary(ValueColumn<T>& left, DictionaryColumn<T>& right, std::shared_ptr<JoinContext> context);
    void join_value_reference(ValueColumn<T>& left, ReferenceColumn& right, std::shared_ptr<JoinContext> context);
    void join_dictionary_dictionary(DictionaryColumn<T>& left, DictionaryColumn<T>& right,
                                    std::shared_ptr<JoinContext> context);
    void join_dictionary_reference(DictionaryColumn<T>& left, ReferenceColumn& right,
                                   std::shared_ptr<JoinContext> context);
    void join_reference_reference(ReferenceColumn& left, ReferenceColumn& right, std::shared_ptr<JoinContext> context);

   private:
    NestedLoopJoin& _nested_loop_join;
    std::function<bool(T&, T&)> _compare;
  };

  std::string& _left_column_name;
  std::string& _right_column_name;
  std::string& _op;

  std::shared_ptr<PosList> _pos_list_left;
  std::shared_ptr<PosList> _pos_list_right;
  std::shared_ptr<Table> _output;
};
}  // namespace opossum
