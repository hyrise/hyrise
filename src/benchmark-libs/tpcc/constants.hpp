#pragma once

/**
 * This file is loosely related to src/scripts/constants.py and contains constants specified in the TPCC benchmark
 * These constants are used for the hyriseBenchmarkTPCC-Suite
 */

#include <stdint.h>

namespace tpcc {

constexpr int32_t NUM_DISTRICTS_PER_WAREHOUSE = 10;
constexpr int32_t NUM_CUSTOMERS_PER_DISTRICT = 3000;
constexpr int32_t MIN_ORDER_LINE_COUNT = 5;
constexpr int32_t MAX_ORDER_LINE_COUNT = 15;
constexpr int32_t NUM_ITEMS = 100000;
constexpr int32_t NUM_STOCK_ITEMS = 100000;
constexpr int32_t NUM_HISTORY_ENTRIES = 1;
constexpr int32_t NUM_ORDERS = 3000;
constexpr int32_t NUM_NEW_ORDERS = 900;
constexpr int32_t MAX_ORDER_LINE_QUANTITY = 10;
constexpr int32_t MIN_CARRIER_ID = 1;
constexpr int32_t MAX_CARRIER_ID = 10;
constexpr float CUSTOMER_YTD = 10;

}  // namespace tpcc
