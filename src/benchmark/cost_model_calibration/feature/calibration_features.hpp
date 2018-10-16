#pragma once

#include <json.hpp>

#include <string>

#include "constant_mappings.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

    struct CalibrationFeatures {
        long long int execution_time_ns = 0;
        double input_table_size_ratio = 0.0;

        size_t left_input_row_count = 0;
        size_t left_input_chunk_count = 0;
        size_t left_input_memory_usage_bytes = 0;
        size_t left_input_chunk_size = 0;

        size_t right_input_row_count = 0;
        size_t right_input_chunk_count = 0;
        size_t right_input_memory_usage_bytes = 0;
        size_t right_input_chunk_size = 0;

        size_t output_row_count = 0;
        size_t output_chunk_count = 0;
        size_t output_memory_usage_bytes = 0;
        size_t output_chunk_size = 0;
        double output_selectivity = 0.0;
    };

    inline void to_json(nlohmann::json& j, const CalibrationFeatures& s) {
        j = nlohmann::json{
                {"execution_time_ns", s.execution_time_ns},
                {"input_table_size_ratio", s.input_table_size_ratio},

                {"left_input_row_count", s.left_input_row_count},
                {"left_input_chunk_count", s.left_input_chunk_count},
                {"left_input_memory_usage_bytes", s.left_input_memory_usage_bytes},
                {"left_input_chunk_size", s.left_input_chunk_size},

                {"right_input_row_count", s.right_input_row_count},
                {"right_input_chunk_count", s.right_input_chunk_count},
                {"right_input_memory_usage_bytes", s.right_input_memory_usage_bytes},
                {"right_input_chunk_size", s.right_input_chunk_size},

                {"output_row_count", s.output_row_count},
                {"output_chunk_count", s.output_chunk_count},
                {"output_memory_usage_bytes", s.output_memory_usage_bytes},
                {"output_chunk_size", s.output_chunk_size},
                {"output_selectivity", s.output_selectivity}
        };
    }

    inline void from_json(const nlohmann::json& j, CalibrationFeatures& s) {
        s.execution_time_ns = j.value("execution_time_ns", 0);
        s.input_table_size_ratio = j.value("input_table_size_ratio", 0);

        s.left_input_row_count = j.value("left_input_row_count", 0);
        s.left_input_chunk_count = j.value("left_input_chunk_count", 0);
        s.left_input_memory_usage_bytes = j.value("left_input_memory_usage_bytes", 0);
        s.left_input_chunk_size = j.value("left_input_chunk_size", 0);

        s.right_input_row_count = j.value("right_input_row_count", 0);
        s.right_input_chunk_count = j.value("right_input_chunk_count", 0);
        s.right_input_memory_usage_bytes = j.value("right_input_memory_usage_bytes", 0);
        s.right_input_chunk_size = j.value("right_input_chunk_size", 0);

        s.output_row_count = j.value("output_row_count", 0);
        s.output_chunk_count = j.value("output_chunk_count", 0);
        s.output_memory_usage_bytes = j.value("output_memory_usage_bytes", 0);
        s.output_chunk_size = j.value("output_chunk_size", 0);
        s.output_selectivity = j.value("output_selectivity", 0);
    }

}  // namespace opossum
