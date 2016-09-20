#pragma once

#include <memory>

#include "abstract_operator.hpp"

namespace opossum {

template <typename T>
class SortImpl;

class Sort : public AbstractOperator {
 public:
  Sort(const std::shared_ptr<AbstractOperator> in, const std::string &sort_column_name, const bool ascending = true);
  virtual void execute();
  virtual std::shared_ptr<Table> get_output() const;

 protected:
  virtual const std::string get_name() const;
  virtual uint8_t get_num_in_tables() const;
  virtual uint8_t get_num_out_tables() const;

  const std::unique_ptr<AbstractOperatorImpl> _impl;
};

template <typename T>
class SortImpl : public AbstractOperatorImpl {
 public:
  SortImpl(const std::shared_ptr<AbstractOperator> in, const std::string &sort_column_name, const bool ascending = true)
      : _sort_column_id(in->get_output()->get_column_id_by_name(sort_column_name)),
        _ascending(ascending),
        _output(new Table),
        _pos_list(new PosList) {}

  virtual void execute() {

  }

  virtual std::shared_ptr<Table> get_output() const;

  const size_t _sort_column_id;
  const bool _ascending;
  std::shared_ptr<Table> _output;
  std::shared_ptr<PosList> _pos_list;
};
}