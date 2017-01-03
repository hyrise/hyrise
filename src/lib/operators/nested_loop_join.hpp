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

   private:
    NestedLoopJoin& _nested_loop_join;
    std::function<bool(T&, T&)> _compare;
  };

  std::string& _left_column_name;
  std::string& _right_column_name;
  std::string& _op;

  std::shared_ptr<Table> _output;
};
}  // namespace opossum
