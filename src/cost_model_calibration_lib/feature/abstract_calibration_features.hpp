#pragma once

#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

    struct AbstractCalibrationFeatures {
        static const std::vector<std::string> columns;

        static const std::vector<std::string> feature_names(const std::optional<std::string>& prefix = {});
        static const std::vector<AllTypeVariant> serialize(const std::optional<CalibrationAggregateFeatures>& features);
    };

    inline const std::vector<std::string> AbstractCalibrationFeatures::feature_names(const std::optional<std::string>& prefix) {
        const auto copied_columns = columns;
        if (prefix) {
            std::for_each(copied_columns.begin(), copied_columns.end(), [prefix](auto& s){ s.insert(0, prefix);});
        }
        return copied_columns;
    };

}  // namespace opossum
