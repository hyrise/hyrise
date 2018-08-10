#pragma once

#include <json.hpp>
#include <string>
#include <vector>

#include <operators/abstract_operator.hpp>
#include <cost_model_calibration/configuration/calibration_configuration.hpp>

namespace opossum {

class CostModelCalibration {

public:
    explicit CostModelCalibration(CalibrationConfiguration configuration);

    void calibrate();

private:
    void _traverse(const std::shared_ptr<const AbstractOperator> & op);
    void _printOperator(const std::shared_ptr<const AbstractOperator> & op);

    nlohmann::json _operators;
    CalibrationConfiguration _configuration;
};

}  // namespace opossum