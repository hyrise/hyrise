#pragma once

/**
 * This file contains constants specified in the TPCH benchmark.
 */

#include <stdint.h>

namespace tpch {

constexpr int32_t NUM_SUPPLIERS = 10000;  // * _scale_factor
constexpr int32_t NUM_PARTS = 200000;     // * _scale_factor
constexpr int32_t NUM_PARTSUPPS_PER_PART = 4;
constexpr int32_t NUM_CUSTOMERS = 150000;  // * _scale_factor
constexpr int32_t NUM_ORDERS_PER_CUSTOMER = 10;
constexpr int32_t NUM_NATIONS = 25;
constexpr int32_t NUM_REGIONS = 5;

}  // namespace tpch
