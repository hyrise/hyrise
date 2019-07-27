#include "aggregate_verification.hpp"

#include <algorithm>
#include <numeric>

#include "lossless_cast.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "storage/table.hpp"

namespace {

using namespace opossum;  // NOLINT

struct BaseAggregate {
  virtual ~BaseAggregate() = default;
  virtual void add_value(const std::optional<AllTypeVariant>& value) = 0;
  virtual AllTypeVariant result() const = 0;
};

template <typename SourceColumnDataType>
struct SumAggregate : public BaseAggregate {
  using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Sum>::AggregateType;

  void add_value(const std::optional<AllTypeVariant>& value) override {
    if (!variant_is_null(*value)) {
      if (!sum) {
        sum.emplace(AggregateType{0});
      }

      *sum += boost::get<SourceColumnDataType>(*value);
    }
  }

  AllTypeVariant result() const override {
    if (sum) {
      return *sum;
    } else {
      return NullValue{};
    }
  }

  std::optional<AggregateType> sum;
};

template <typename SourceColumnDataType>
struct MinAggregate : public BaseAggregate {
  void add_value(const std::optional<AllTypeVariant>& value) override {
    if (variant_is_null(*value)) return;
    if (min) {
      min = std::min(*min, boost::get<SourceColumnDataType>(*value));
    } else {
      min = boost::get<SourceColumnDataType>(*value);
    }
  }

  AllTypeVariant result() const override { return min ? *min : AllTypeVariant{}; }

  std::optional<typename AggregateTraits<SourceColumnDataType, AggregateFunction::Min>::AggregateType> min;
};

template <typename SourceColumnDataType>
struct MaxAggregate : public BaseAggregate {
  void add_value(const std::optional<AllTypeVariant>& value) override {
    if (variant_is_null(*value)) return;
    if (max) {
      max = std::max(*max, boost::get<SourceColumnDataType>(*value));
    } else {
      max = boost::get<SourceColumnDataType>(*value);
    }
  }

  AllTypeVariant result() const override { return max ? *max : AllTypeVariant{}; }

  std::optional<typename AggregateTraits<SourceColumnDataType, AggregateFunction::Max>::AggregateType> max;
};

template <typename SourceColumnDataType>
struct AvgAggregate : public BaseAggregate {
  void add_value(const std::optional<AllTypeVariant>& value) override {
    if (variant_is_null(*value)) return;
    values.emplace_back(boost::get<SourceColumnDataType>(*value));
  }

  AllTypeVariant result() const override {
    using AggregateType = typename AggregateTraits<SourceColumnDataType, AggregateFunction::Avg>::AggregateType;

    if (values.empty()) {
      return NullValue{};
    }

    return std::accumulate(values.begin(), values.end(), AggregateType{0}) / values.size();
  }

  std::vector<SourceColumnDataType> values;
};

template <typename SourceColumnDataType>
struct CountDistinctAggregate : public BaseAggregate {
  void add_value(const std::optional<AllTypeVariant>& value) override {
    if (variant_is_null(*value)) return;
    distinct_values.emplace(boost::get<SourceColumnDataType>(*value));
  }

  AllTypeVariant result() const override {
    using AggregateType =
        typename AggregateTraits<SourceColumnDataType, AggregateFunction::CountDistinct>::AggregateType;
    return static_cast<AggregateType>(distinct_values.size());
  }

  std::unordered_set<SourceColumnDataType> distinct_values;
};

template <typename SourceColumnDataType>
struct CountNonNullAggregate : public BaseAggregate {
  void add_value(const std::optional<AllTypeVariant>& value) override {
    if (!variant_is_null(*value)) {
      ++count;
    }
  }

  AllTypeVariant result() const override {
    using AggregateType =
        typename AggregateTraits<SourceColumnDataType, AggregateFunction::CountNonNull>::AggregateType;
    return static_cast<AggregateType>(count);
  }

  size_t count{0};
};

struct CountRowsAggregate : public BaseAggregate {
  void add_value(const std::optional<AllTypeVariant>& value) override { ++count; }

  AllTypeVariant result() const override { return static_cast<int64_t>(count); }

  size_t count{0};
};

std::unique_ptr<BaseAggregate> make_aggregate(const Table& table,
                                              const AggregateColumnDefinition& aggregate_column_definition) {
  if (aggregate_column_definition.function == AggregateFunction::CountRows) {
    return std::make_unique<CountRowsAggregate>();
  }

  auto aggregate = std::unique_ptr<BaseAggregate>{};

  resolve_data_type(table.column_data_type(*aggregate_column_definition.column),
                    [&](const auto source_column_data_type_t) {
                      using SourceColumnDataType = typename decltype(source_column_data_type_t)::type;
                      switch (aggregate_column_definition.function) {
                        case AggregateFunction::Min:
                          aggregate = std::make_unique<MinAggregate<SourceColumnDataType>>();
                          break;
                        case AggregateFunction::Max:
                          aggregate = std::make_unique<MaxAggregate<SourceColumnDataType>>();
                          break;
                        case AggregateFunction::Sum:
                          if constexpr (!std::is_same_v<SourceColumnDataType, pmr_string>) {
                            aggregate = std::make_unique<SumAggregate<SourceColumnDataType>>();
                          } else {
                            Fail("SUM(<string column>) not implemented");
                          }
                          break;
                        case AggregateFunction::Avg:
                          if constexpr (!std::is_same_v<SourceColumnDataType, pmr_string>) {
                            aggregate = std::make_unique<AvgAggregate<SourceColumnDataType>>();
                          } else {
                            Fail("AVG(<string column>) not implemented");
                          }
                          break;
                        case AggregateFunction::StandardDeviationSample:
                          Fail("STDDEV_SAMPLE() not implemented");
                        case AggregateFunction::CountNonNull:
                          aggregate = std::make_unique<CountNonNullAggregate<SourceColumnDataType>>();
                          break;
                        case AggregateFunction::CountDistinct:
                          aggregate = std::make_unique<CountDistinctAggregate<SourceColumnDataType>>();
                          break;
                        case AggregateFunction::CountRows:
                          Fail("Handled above");
                          break;
                      }
                    });

  return aggregate;
}

std::vector<std::unique_ptr<BaseAggregate>> make_group(
    const Table& table, const std::vector<AggregateColumnDefinition>& aggregate_column_definitions) {
  auto aggregates = std::vector<std::unique_ptr<BaseAggregate>>(aggregate_column_definitions.size());
  std::transform(
      aggregate_column_definitions.begin(), aggregate_column_definitions.end(), aggregates.begin(),
      [&](const auto& aggregate_column_definition) { return make_aggregate(table, aggregate_column_definition); });
  return aggregates;
}

}  // namespace

namespace opossum {

AggregateVerification::AggregateVerification(const std::shared_ptr<AbstractOperator>& in,
                                             const std::vector<AggregateColumnDefinition>& aggregate_column_definitions,
                                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(in, aggregate_column_definitions, groupby_column_ids) {}

const std::string AggregateVerification::name() const { return "AggregateVerification"; }

std::shared_ptr<const Table> AggregateVerification::_on_execute() {
  const auto rows = input_table_left()->get_rows();

  auto groups = std::map<std::vector<AllTypeVariant>, std::vector<std::unique_ptr<BaseAggregate>>>{};

  for (const auto& row : rows) {
    auto group_by_values = std::vector<AllTypeVariant>(_groupby_column_ids.size());
    for (auto group_by_idx = size_t{0}; group_by_idx < _groupby_column_ids.size(); ++group_by_idx) {
      group_by_values[group_by_idx] = row[_groupby_column_ids[group_by_idx]];
    }

    auto group_iter = groups.find(group_by_values);
    if (group_iter == groups.end()) {
      group_iter = groups.emplace(std::move(group_by_values), make_group(*input_table_left(), _aggregates)).first;
    }

    for (auto aggregate_idx = size_t{0}; aggregate_idx < _aggregates.size(); ++aggregate_idx) {
      if (_aggregates[aggregate_idx].column) {
        group_iter->second[aggregate_idx]->add_value(row[*_aggregates[aggregate_idx].column]);
      } else {
        group_iter->second[aggregate_idx]->add_value(std::nullopt);
      }
    }
  }

  if (groups.empty() && _groupby_column_ids.empty()) {
    groups.emplace(std::vector<AllTypeVariant>{}, make_group(*input_table_left(), _aggregates));
  }

  const auto output_table = std::make_shared<Table>(_get_output_column_defintions(), TableType::Data);

  for (const auto& [group_by_values, aggregates] : groups) {
    auto output_row = std::vector<AllTypeVariant>(_groupby_column_ids.size() + _aggregates.size());

    auto aggregate_values_iter = std::copy(group_by_values.begin(), group_by_values.end(), output_row.begin());
    for (const auto& aggregate : aggregates) {
      *aggregate_values_iter = aggregate->result();
      ++aggregate_values_iter;
    }

    output_table->append(output_row);
  }

  return output_table;
}

std::shared_ptr<AbstractOperator> AggregateVerification::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<AggregateVerification>(copied_input_left, _aggregates, _groupby_column_ids);
}

void AggregateVerification::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateVerification::_on_cleanup() {}

}  // namespace opossum