#pragma once

#include <atomic>
#include <memory>

#include "abstract_read_only_operator.hpp"
#include "operator_join_predicate.hpp"
#include "types.hpp"

#include "utils/bloom_filter.hpp"
#include "utils/min_max_filter.hpp"

namespace hyrise {

class Reduce : public AbstractReadOnlyOperator {

 public:
  explicit Reduce(const std::shared_ptr<const AbstractOperator>& left_input,
                  const std::shared_ptr<const AbstractOperator>& right_input)
                  : AbstractReadOnlyOperator{OperatorType::Reduce, left_input, right_input} {
    _bloom_filter = std::make_shared<BloomFilter<20, 2>>();
    _min_max_filter = std::make_shared<MinMaxFilter>();
  }

  const std::string& name() const override {
    static const auto name = std::string{"Reduce"};
    return name;
  }

 protected:
  std::shared_ptr<const Table> _on_execute() override {
    return nullptr;
  }

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override {}

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override {
        return std::make_shared<Reduce>(copied_left_input, copied_right_input);
      }

  std::shared_ptr<BloomFilter<20, 2>> _bloom_filter;
  std::shared_ptr<MinMaxFilter> _min_max_filter;
};

}  // namespace hyrise