#pragma once

#include <string>

#include "json.hpp"


namespace opossum {

enum class CalibrationType { CompleteTableScan, FilteredTableScan, Materialization };

std::string to_string(CalibrationType type);

nlohmann::json generate_calibration_description(CalibrationType type);

}
