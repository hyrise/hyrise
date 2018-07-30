#pragma once

#include <json.hpp>
#include <string>
#include <vector>

#include <operators/abstract_operator.hpp>

namespace opossum {

class CostModelCalibration {

public:
    explicit CostModelCalibration(const nlohmann::json& configuration);

    void calibrate();

private:
    const std::vector<std::string> _generateQueries(const nlohmann::json& table_definitions);
    void _traverse(const std::shared_ptr<const AbstractOperator> & op);
    void _printOperator(const std::shared_ptr<const AbstractOperator> & op);

    nlohmann::json _operators;
    nlohmann::json _configuration;
};

}  // namespace opossum