#pragma once

#include <nlohmann/json.hpp>

#include <string>

#include "constant_mappings.hpp"
#include "storage/base_segment_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/vector_compression/vector_compression.hpp"

namespace opossum {

struct CalibrationColumnSpecification {
  std::string column_name;
  ColumnID column_id;
  DataType data_type;
  std::string value_distribution;
  bool sorted;
  size_t distinct_value_count;
  SegmentEncodingSpec encoding;
  size_t fraction;
};

inline void to_json(nlohmann::json& j, const CalibrationColumnSpecification& s) {
  std::string vector_compression = "None";
  if (s.encoding.vector_compression_type) {
    vector_compression = vector_compression_type_to_string.left.at(*s.encoding.vector_compression_type);
  }
  j = nlohmann::json{{"column_name", s.column_name},
                     {"data_type", s.data_type},
                     {"value_distribution", s.value_distribution},
                     {"sorted", s.sorted},
                     {"distinct_value_count", s.distinct_value_count},
                     {"encoding", s.encoding.encoding_type},
                     {"vector_compression", vector_compression},
                     {"fraction", s.fraction}};
}

inline bool operator==(const CalibrationColumnSpecification& lhs, const CalibrationColumnSpecification& rhs) {
  return std::tie(lhs.column_name, lhs.data_type, lhs.value_distribution, lhs.sorted, lhs.distinct_value_count,
                  lhs.encoding.encoding_type, lhs.encoding.vector_compression_type, lhs.fraction) ==
         std::tie(rhs.column_name, rhs.data_type, rhs.value_distribution, rhs.sorted, rhs.distinct_value_count,
                  rhs.encoding.encoding_type, rhs.encoding.vector_compression_type, rhs.fraction);
}

inline void from_json(const nlohmann::json& j, CalibrationColumnSpecification& s) {
  auto data_type_string = j.value("type", "int");
  if (data_type_to_string.right.find(data_type_string) == data_type_to_string.right.end()) {
    Fail("Unsupported data type");
  }
  s.column_name = j.at("column_name").get<std::string>();
  s.data_type = data_type_to_string.right.at(data_type_string);
  s.value_distribution = j.value("value_distribution", "uniform");
  s.sorted = j.value("sorted", false);
  s.distinct_value_count = j.value("distinct_values", 0);

  auto encoding_string = j.value("encoding", "Unencoded");
  auto vector_compression_string = j.value("vector_compression", "None");

  // Check if specified encodings and vector compressions are valid
  if (encoding_type_to_string.right.find(encoding_string) == encoding_type_to_string.right.end()) {
    Fail("Unsupported encoding.");
  }
  if (vector_compression_string != "None" && vector_compression_type_to_string.right.find(vector_compression_string) ==
                                                 vector_compression_type_to_string.right.end()) {
    Fail("Unsupported vector compression.");
  }

  // Check if encoding and vector compression are specified, but encoding does not support vector compression.
  if (encoding_type_to_string.right.at(encoding_string) != EncodingType::Unencoded &&
      vector_compression_string != "None") {
    auto encoder = create_encoder(encoding_type_to_string.right.at(encoding_string));
    if (!encoder->uses_vector_compression()) {
      Fail("Encoding type does not support vector compression.");
    }
  }

  s.encoding = SegmentEncodingSpec{encoding_type_to_string.right.at(encoding_string)};
  if (vector_compression_string != "None") {
    s.encoding.vector_compression_type = vector_compression_type_to_string.right.at(vector_compression_string);
  }
  s.fraction = j.value("fraction", 0);
}

}  // namespace opossum
