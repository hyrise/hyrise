#pragma once

#include <stdint.h>

namespace tpcc {

constexpr int32_t NUM_DISTRICTS_PER_WAREHOUSE = 10;
constexpr int32_t NUM_CUSTOMERS_PER_DISTRICT = 3000;
constexpr int32_t MIN_ORDER_LINE_COUNT = 5;
constexpr int32_t MAX_ORDER_LINE_COUNT = 15;
constexpr int32_t NUM_ITEMS = 100000;
constexpr int32_t MAX_ORDER_LINE_QUANTITY = 10;
constexpr int32_t MIN_CARRIER_ID = 1;
constexpr int32_t MAX_CARRIER_ID = 10;

}  // namespace tpcc
