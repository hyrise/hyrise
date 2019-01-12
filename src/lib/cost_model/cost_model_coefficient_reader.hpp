#pragma once

#include <string>

#include "cost_model/cost_model_adaptive.hpp"

namespace opossum {

    class CostModelCoefficientReader {
    public:
        static const ModelCoefficientsPerGroup read_coefficients(const std::string& file_path);
    };

}  // namespace opossum
