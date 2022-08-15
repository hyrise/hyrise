#pragma once

#include "operators/abstract_operator.hpp"
#include "strong_typedef.hpp"
#include "types.hpp"

STRONG_TYPEDEF(double, Feature);

namespace hyrise {

enum class QueryOperatorType {
  Aggregate,
  Alias,
  Difference,
  GetTable,
  IndexScan,
  JoinHash,
  JoinIndex,
  Limit,
  Product,
  Projection,
  Sort,
  TableScan,
  TableWrapper,
  UnionAll,
  UnionPositions,
  Validate,
};

const auto operator_type_mapping = std::unordered_map<OperatorType, QueryOperatorType>{
    {OperatorType::Aggregate, QueryOperatorType::Aggregate},
    {OperatorType::Alias, QueryOperatorType::Alias},
    {OperatorType::Difference, QueryOperatorType::Difference},
    {OperatorType::GetTable, QueryOperatorType::GetTable},
    {OperatorType::IndexScan, QueryOperatorType::IndexScan},
    {OperatorType::JoinHash, QueryOperatorType::JoinHash},
    {OperatorType::JoinIndex, QueryOperatorType::JoinIndex},
    {OperatorType::Limit, QueryOperatorType::Limit},
    {OperatorType::Product, QueryOperatorType::Product},
    {OperatorType::Projection, QueryOperatorType::Projection},
    {OperatorType::Sort, QueryOperatorType::Sort},
    {OperatorType::TableScan, QueryOperatorType::TableScan},
    {OperatorType::TableWrapper, QueryOperatorType::TableWrapper},
    {OperatorType::UnionAll, QueryOperatorType::UnionAll},
    {OperatorType::UnionPositions, QueryOperatorType::UnionPositions},
    {OperatorType::Validate, QueryOperatorType::Validate}};

QueryOperatorType map_operator_type(const OperatorType operator_type);

struct Query {
  Query(const std::string& init_hash, const std::string& init_query, const size_t init_frequency)
      : hash{init_hash}, query{init_query}, frequency{init_frequency} {}
  std::string hash;
  std::string query;
  size_t frequency;
};

using FeatureVector = std::vector<Feature>;

void feature_vector_to_stream(const FeatureVector& feature_vector, std::ostream& stream);

std::string feature_vector_to_string(const FeatureVector& feature_vector);

std::ostream& operator<<(std::ostream& stream, const FeatureVector& feature_vector);

}  // namespace hyrise
