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
    const std::vector<std::string> _queries{
            "SELECT column_a FROM SomeTable;",
            "SELECT column_b FROM SomeTable;",
            "SELECT column_c FROM SomeTable;",
            "SELECT column_a, column_b, column_c FROM SomeTable;",
            "SELECT * FROM SomeTable;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a = 753;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a = 345;",
            "SELECT column_a, column_b, column_c, column_d FROM SomeTable WHERE column_d = 4;",
            "SELECT column_a, column_b, column_c, column_d FROM SomeTable WHERE column_d = 7;",
            "SELECT column_a, column_b, column_c, column_d FROM SomeTable WHERE column_d = 9;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a < 200;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a < 600;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a < 900;",
            "SELECT column_b FROM SomeTable WHERE column_b < 'Bradley Davis';",
            "SELECT COUNT(*) FROM SomeTable"
    };

    void _traverse(const std::shared_ptr<const AbstractOperator> & op);
    void _printOperator(const std::shared_ptr<const AbstractOperator> & op);

    nlohmann::json _operators;
    nlohmann::json _configuration;
};

}  // namespace opossum