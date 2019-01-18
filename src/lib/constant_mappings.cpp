#include <boost/hana/fold.hpp>

#include "constant_mappings.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/function_expression.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "storage/vector_compression/vector_compression.hpp"

namespace opossum {

const Bimap<PredicateCondition, std::string> predicate_condition_to_string =
    Bimap<PredicateCondition, std::string>({
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

const Bimap<AggregateFunction, std::string> aggregate_function_to_string =
    Bimap<AggregateFunction, std::string>({
        {AggregateFunction::Min, "MIN"},
        {AggregateFunction::Max, "MAX"},
        {AggregateFunction::Sum, "SUM"},
        {AggregateFunction::Avg, "AVG"},
        {AggregateFunction::Count, "COUNT"},
        {AggregateFunction::CountDistinct, "COUNT DISTINCT"},
    });

const Bimap<FunctionType, std::string> function_type_to_string =
    Bimap<FunctionType, std::string>({{FunctionType::Substring, "SUBSTR"}, {FunctionType::Concatenate, "CONCAT"}});

const Bimap<DataType, std::string> data_type_to_string =
    hana::fold(data_type_enum_string_pairs, Bimap<DataType, std::string>{}, [](auto map, auto pair) {
      map.insert({hana::first(pair), std::string{hana::second(pair)}});
      return map;
    });

const Bimap<EncodingType, std::string> encoding_type_to_string = Bimap<EncodingType, std::string>({
    {EncodingType::Dictionary, "Dictionary"},
    {EncodingType::RunLength, "RunLength"},
    {EncodingType::FixedStringDictionary, "FixedStringDictionary"},
    {EncodingType::FrameOfReference, "FrameOfReference"},
    {EncodingType::Unencoded, "Unencoded"},
});

const Bimap<VectorCompressionType, std::string> vector_compression_type_to_string =
    Bimap<VectorCompressionType, std::string>({
        {VectorCompressionType::FixedSizeByteAligned, "Fixed-size byte-aligned"},
        {VectorCompressionType::SimdBp128, "SIMD-BP128"},
    });

const Bimap<TableType, std::string> table_type_to_string =
    Bimap<TableType, std::string>({{TableType::Data, "Data"}, {TableType::References, "References"}});

}  // namespace opossum
