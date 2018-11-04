#include "constant_mappings.hpp"

#include <boost/bimap.hpp>
#include <boost/hana/fold.hpp>

#include <string>
#include <unordered_map>

#include "sql/Expr.h"
#include "sql/SelectStatement.h"

#include "expression/abstract_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "storage/vector_compression/vector_compression.hpp"

namespace opossum {

/*
 * boost::bimap does not support initializer_lists.
 * Instead we use this helper function to have an initializer_list-friendly interface.
 */
template <typename L, typename R>
boost::bimap<L, R> make_bimap(std::initializer_list<typename boost::bimap<L, R>::value_type> list) {
  return boost::bimap<L, R>(list.begin(), list.end());
}

const boost::bimap<PredicateCondition, std::string> predicate_condition_to_string =
    make_bimap<PredicateCondition, std::string>({
        {PredicateCondition::Equals, "="},
        {PredicateCondition::NotEquals, "!="},
        {PredicateCondition::LessThan, "<"},
        {PredicateCondition::LessThanEquals, "<="},
        {PredicateCondition::GreaterThan, ">"},
        {PredicateCondition::GreaterThanEquals, ">="},
        {PredicateCondition::Between, "BETWEEN"},
        {PredicateCondition::Like, "LIKE"},
        {PredicateCondition::NotLike, "NOT LIKE"},
        {PredicateCondition::In, "IN"},
        {PredicateCondition::NotIn, "NOT IN"},
        {PredicateCondition::IsNull, "IS NULL"},
        {PredicateCondition::IsNotNull, "IS NOT NULL"},
    });

const std::unordered_map<OrderByMode, std::string> order_by_mode_to_string = {
    {OrderByMode::Ascending, "Ascending"},
    {OrderByMode::Descending, "Descending"},
};

const std::unordered_map<hsql::OrderType, OrderByMode> order_type_to_order_by_mode = {
    {hsql::kOrderAsc, OrderByMode::Ascending},
    {hsql::kOrderDesc, OrderByMode::Descending},
};

const std::unordered_map<JoinMode, std::string> join_mode_to_string = {
    {JoinMode::Cross, "Cross"}, {JoinMode::Inner, "Inner"}, {JoinMode::Left, "Left"}, {JoinMode::Outer, "Outer"},
    {JoinMode::Right, "Right"}, {JoinMode::Semi, "Semi"},   {JoinMode::Anti, "Anti"},
};

const std::unordered_map<UnionMode, std::string> union_mode_to_string = {{UnionMode::Positions, "UnionPositions"}};

const boost::bimap<AggregateFunction, std::string> aggregate_function_to_string =
    make_bimap<AggregateFunction, std::string>({
        {AggregateFunction::Min, "MIN"},
        {AggregateFunction::Max, "MAX"},
        {AggregateFunction::Sum, "SUM"},
        {AggregateFunction::Avg, "AVG"},
        {AggregateFunction::Count, "COUNT"},
        {AggregateFunction::CountDistinct, "COUNT DISTINCT"},
    });

const boost::bimap<FunctionType, std::string> function_type_to_string =
    make_bimap<FunctionType, std::string>({{FunctionType::Substring, "SUBSTR"}, {FunctionType::Concatenate, "CONCAT"}});

const boost::bimap<DataType, std::string> data_type_to_string =
    hana::fold(data_type_enum_string_pairs, boost::bimap<DataType, std::string>{}, [](auto map, auto pair) {
      map.insert({hana::first(pair), std::string{hana::second(pair)}});
      return map;
    });

const boost::bimap<EncodingType, std::string> encoding_type_to_string = make_bimap<EncodingType, std::string>({
    {EncodingType::Dictionary, "Dictionary"},
    {EncodingType::RunLength, "RunLength"},
    {EncodingType::FixedStringDictionary, "FixedStringDictionary"},
    {EncodingType::FrameOfReference, "FrameOfReference"},
    {EncodingType::Unencoded, "Unencoded"},
});

const boost::bimap<VectorCompressionType, std::string> vector_compression_type_to_string =
    make_bimap<VectorCompressionType, std::string>({
        {VectorCompressionType::FixedSizeByteAligned, "Fixed-size byte-aligned"},
        {VectorCompressionType::SimdBp128, "SIMD-BP128"},
    });

const boost::bimap<TableType, std::string> table_type_to_string =
    make_bimap<TableType, std::string>({{TableType::Data, "Data"}, {TableType::References, "References"}});

const boost::bimap<JitExpressionType, std::string> jit_expression_type_to_string =
    make_bimap<JitExpressionType, std::string>({{JitExpressionType::Addition, "+"},
                                                {JitExpressionType::Column, "<COLUMN>"},
                                                {JitExpressionType::Subtraction, "-"},
                                                {JitExpressionType::Multiplication, "*"},
                                                {JitExpressionType::Division, "/"},
                                                {JitExpressionType::Modulo, "%"},
                                                {JitExpressionType::Power, "^"},
                                                {JitExpressionType::Equals, "="},
                                                {JitExpressionType::NotEquals, "<>"},
                                                {JitExpressionType::GreaterThan, ">"},
                                                {JitExpressionType::GreaterThanEquals, ">="},
                                                {JitExpressionType::LessThan, "<"},
                                                {JitExpressionType::LessThanEquals, "<="},
                                                {JitExpressionType::Like, "LIKE"},
                                                {JitExpressionType::NotLike, "NOT LIKE"},
                                                {JitExpressionType::And, "AND"},
                                                {JitExpressionType::Or, "OR"},
                                                {JitExpressionType::Not, "NOT"},
                                                {JitExpressionType::IsNull, "IS NULL"},
                                                {JitExpressionType::IsNotNull, "IS NOT NULL"}});

}  // namespace opossum
