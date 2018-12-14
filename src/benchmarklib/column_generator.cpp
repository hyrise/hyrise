
#include <memory>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "boost/math/distributions/pareto.hpp"
#include "boost/math/distributions/skew_normal.hpp"
#include "boost/math/distributions/uniform.hpp"

#include "types.hpp"

#include "column_generator.hpp"
#include "table_generator.hpp"

#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"


namespace opossum {

ColumnGenerator::ColumnGenerator() : _probability_dist(0.0, 1.0) {
  std::random_device rd;
  _pseudorandom_engine.seed(rd());
}

// Todo: Refactor TableGenerator::generate_table to uses this function
template<typename VectorType>
VectorType ColumnGenerator::generate_value_vector(const ColumnDataDistribution& column_data_distribution,
    size_t row_count, const std::function<bool(size_t)>& allow_value) {

  VectorType result(row_count);

  auto generate_value_by_distribution_type = std::function<int(void)>{};

  // generate distribution from column configuration
  switch (column_data_distribution.distribution_type) {
    case DataDistributionType::Uniform: {
      auto uniform_dist = boost::math::uniform_distribution<double>{column_data_distribution.min_value,
                                                                    column_data_distribution.max_value};
      generate_value_by_distribution_type = [&]() {
        const auto probability = _probability_dist(_pseudorandom_engine);
        return static_cast<int>(std::floor(boost::math::quantile(uniform_dist, probability)));
      };
      break;
    }
    case DataDistributionType::NormalSkewed: {
      auto skew_dist = boost::math::skew_normal_distribution<double>{column_data_distribution.skew_location,
                                                                     column_data_distribution.skew_scale,
                                                                     column_data_distribution.skew_shape};
      generate_value_by_distribution_type = [&]() {
        const auto probability = _probability_dist(_pseudorandom_engine);
        return static_cast<int>(std::round(boost::math::quantile(skew_dist, probability) * 10));
      };
      break;
    }
    case DataDistributionType::Pareto: {
      auto pareto_dist = boost::math::pareto_distribution<double>{column_data_distribution.pareto_scale,
                                                                  column_data_distribution.pareto_shape};
      generate_value_by_distribution_type = [&]() {
        const auto probability = _probability_dist(_pseudorandom_engine);
        return static_cast<int>(std::floor(boost::math::quantile(pareto_dist, probability)));
      };
      break;
    }
  }

  for (size_t row_offset{0}; row_offset < row_count; ++row_offset) {
    auto rnd_value = generate_value_by_distribution_type();
    while (!allow_value(rnd_value)) {
      rnd_value = generate_value_by_distribution_type();
    }
    result[row_offset] = rnd_value;
  }

  return result;
}

/**
 * Generates a vector which contains selectivity * row_count values included in unique_ids.
 * The idea is to generate a column where selectivity * row_count entries will find a join partner
 * given by unique_ids.
 * @param unique_values
 * @param row_count
 * @param selectivity
 * @param get_value_with_no_join_partner
 * @return
 */
std::vector<size_t> ColumnGenerator::generate_join_partner(const std::vector<size_t>& join_partner, size_t row_count,
    float selectivity, const std::function<size_t(double)>& get_value_with_no_join_partner) {

  std::vector<size_t> result(row_count);
  auto required_join_partners = static_cast<size_t>(selectivity * row_count);
  auto required_no_join_partners = row_count - required_join_partners;
  const auto select_join_partner_threshold = required_join_partners / row_count;

  size_t selected_rows = 0;
  while (selected_rows < row_count) {
    auto rnd = _probability_dist(_pseudorandom_engine);
    if ((required_join_partners > 0 && rnd < select_join_partner_threshold) || required_no_join_partners == 0) {
      const auto rnd_join_index = static_cast<size_t >(_probability_dist(_pseudorandom_engine) * join_partner.size());
      result[selected_rows] = join_partner[rnd_join_index];
      --required_join_partners;
    }
    else {
      result[selected_rows] = get_value_with_no_join_partner(_probability_dist(_pseudorandom_engine));
      --required_no_join_partners;
    }
    ++selected_rows;
  }

  return result;
}

std::unique_ptr<std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>>>
    ColumnGenerator::generate_join_pair(std::vector<double>& selectivities, size_t chunk_size,
        size_t row_count_table1, size_t row_count_table2) {

  const auto column_count = selectivities.size();

  TableColumnDefinitions column_definitions;
  for (size_t i = 0; i < column_count; i++) {
    auto column_name = std::string(1, static_cast<char>(static_cast<int>('a') + i));
    column_definitions.emplace_back(column_name, DataType::Int);
  }

  auto table1 = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size);
  auto table2 = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size);


  const auto value_distribution = ColumnDataDistribution::make_uniform_config(0.0, 1.0);

  std::vector<std::vector<size_t>> cols_table1;
  std::vector<std::vector<size_t>> cols_table2;
  cols_table1.resize(column_count);
  cols_table2.resize(column_count);

  const auto allow_value = [](size_t value) { return value % 10 != 0; };
  const auto get_value_without_join_partner = [](double value) { return static_cast<size_t>(value * 10); };

  for (size_t col_idx = 0; col_idx < column_count; ++col_idx) {
    const auto values_table1 = generate_value_vector<std::vector<size_t>>(value_distribution, row_count_table1,
        allow_value);
    cols_table1[col_idx] = std::move(values_table1);

    const auto values_table2 = generate_join_partner(cols_table1[col_idx], row_count_table2, selectivities[col_idx],
        get_value_without_join_partner);
    cols_table2[col_idx] = std::move(values_table2);
  }




  return std::make_unique<std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>>>(std::make_pair(table1, table2));
}

} // namespace opossum