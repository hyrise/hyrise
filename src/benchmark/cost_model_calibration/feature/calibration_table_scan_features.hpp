#pragma once

#include <json.hpp>

#include "storage/encoding_type.hpp"

namespace opossum {

    struct CalibrationTableScanFeatures {
        EncodingType scan_segment_encoding = EncodingType::Unencoded;
        bool is_scan_segment_reference_segment = false;
        DataType scan_segment_data_type = DataType::Int; // Just any default
        int scan_segment_memory_usage_bytes = 0;
        int scan_segment_distinct_value_count = 0;
        EncodingType second_scan_segment_encoding = EncodingType::Unencoded;
        bool is_second_scan_segment_reference_segment = false;
        DataType second_scan_segment_data_type = DataType::Int; // Just any default
        int second_scan_segment_memory_usage_bytes = 0;
        int second_scan_segment_distinct_value_count = 0;
        PredicateCondition scan_operator_type = PredicateCondition::Equals;
        std::string scan_operator_description = "";
    };

    inline void to_json(nlohmann::json& j, const CalibrationTableScanFeatures& s) {
        j = nlohmann::json{
                {"scan_segment_encoding", s.scan_segment_encoding},
                {"is_scan_segment_reference_segment", s.is_scan_segment_reference_segment},
                {"scan_segment_data_type", s.scan_segment_data_type},
                {"scan_segment_memory_usage_bytes", s.scan_segment_memory_usage_bytes},
                {"scan_segment_distinct_value_count", s.scan_segment_distinct_value_count},
                {"second_scan_segment_encoding", s.second_scan_segment_encoding},
                {"is_second_scan_segment_reference_segment", s.is_second_scan_segment_reference_segment},
                {"second_scan_segment_data_type", s.second_scan_segment_data_type},
                {"second_scan_segment_memory_usage_bytes", s.second_scan_segment_memory_usage_bytes},
                {"second_scan_segment_distinct_value_count", s.second_scan_segment_distinct_value_count},
                {"scan_operator_type", s.scan_operator_type},
                {"scan_operator_description", s.scan_operator_description}
        };
    }

    inline void from_json(const nlohmann::json& j, CalibrationTableScanFeatures& s) {
        s.scan_segment_encoding = j.value("scan_segment_encoding", EncodingType::Unencoded);
        s.is_scan_segment_reference_segment = j.value("is_scan_segment_reference_segment", false);
        s.scan_segment_data_type = j.value("scan_segment_data_type", DataType::Int);
        s.scan_segment_memory_usage_bytes = j.value("scan_segment_memory_usage_bytes", 0);
        s.scan_segment_distinct_value_count = j.value("scan_segment_distinct_value_count", 0);
        s.second_scan_segment_encoding = j.value("second_scan_segment_encoding", EncodingType::Unencoded);
        s.is_second_scan_segment_reference_segment = j.value("is_second_scan_segment_reference_segment", false);
        s.second_scan_segment_data_type = j.value("second_scan_segment_data_type", DataType::Int);
        s.second_scan_segment_memory_usage_bytes = j.value("second_scan_segment_memory_usage_bytes", 0);
        s.second_scan_segment_distinct_value_count = j.value("second_scan_segment_distinct_value_count", 0);
        s.scan_operator_type = j.value("scan_operator_type", PredicateCondition::Equals);
        s.scan_operator_description = j.value("scan_operator_description", "");
    }

}  // namespace opossum
