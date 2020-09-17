#pragma once

#include <algorithm>
#include <mutex>
#include <numeric>
#include <queue>
#include <thread>

#include "gtest/gtest_prod.h"
#include "hyrise.hpp"
#include "storage/chunk.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "utils/settings/abstract_setting.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class CompressionPlugin : public AbstractPlugin {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;

 private:
  // Budget in megabyte
  class MemoryBudgetSetting : public AbstractSetting {
   public:
    MemoryBudgetSetting() : AbstractSetting("Plugin::Compression::MemoryBudget") {}
    const std::string& description() const final {
      static const auto description = std::string{"The memory budget (MB) to target for the CompressionPlugin."};
      return description;
    }
    const std::string& display_name() const final { return _display_name; }
    const std::string& get() final { return _value; }
    void set(const std::string& value) final { _value = value; }

    std::string _value = "10000";
    std::string _display_name = "Memory Budget (MB)";
  };

  constexpr static std::chrono::milliseconds THREAD_INTERVAL = std::chrono::milliseconds(7'500);

  std::unique_ptr<PausableLoopThread> _loop_thread;

  bool _stop_requested = false;

  int64_t _compress_column(const std::string table_name, const std::string column_name, const std::string encoding_name,
                           const bool column_was_accessed, const int64_t desired_memory_usage_reduction);

  void _optimize_compression();

  std::shared_ptr<MemoryBudgetSetting> _memory_budget_setting;

  // Hand tuned, determined by an greedy "Microsoft-like heuristic". Adapted the use of FSBA and changed it almost
  // everywhere to SIMDBP128 (long story).
  // Update: temporary change to FSBA due to encoding crashes.
  std::vector<std::vector<std::string>> _static_compression_config = {
      {"UNACCESSED", "lineitem_tpch_1", "l_shipinstruct", "FixedStringDictionarySIMDBP128"},
      {"UNACCESSED", "lineitem_tpch_0_1", "l_shipinstruct", "FixedStringDictionarySIMDBP128"},
      {"UNACCESSED", "lineitem_tpch_1", "l_comment", "LZ4SIMDBP128"},
      {"UNACCESSED", "lineitem_tpch_0_1", "l_comment", "LZ4SIMDBP128"},
      {"UNACCESSED", "nation_tpch_1", "n_comment", "FixedStringDictionarySIMDBP128"},
      {"UNACCESSED", "nation_tpch_0_1", "n_comment", "FixedStringDictionarySIMDBP128"},
      {"UNACCESSED", "part_tpch_1", "p_comment", "LZ4SIMDBP128"},
      {"UNACCESSED", "part_tpch_0_1", "p_comment", "LZ4SIMDBP128"},
      {"UNACCESSED", "lineitem_tpch_1", "l_linenumber", "FrameOfReferenceSIMDBP128"},
      {"UNACCESSED", "lineitem_tpch_0_1", "l_linenumber", "FrameOfReferenceSIMDBP128"},
      {"UNACCESSED", "orders_tpch_1", "o_clerk", "LZ4SIMDBP128"},
      {"UNACCESSED", "orders_tpch_0_1", "o_clerk", "LZ4SIMDBP128"},
      {"UNACCESSED", "part_tpch_1", "p_retailsize", "DictionarySIMDBP128"},
      {"UNACCESSED", "part_tpch_0_1", "p_retailsize", "DictionarySIMDBP128"},
      {"UNACCESSED", "partsupp_tpch_1", "ps_comment", "LZ4SIMDBP128"},
      {"UNACCESSED", "partsupp_tpch_0_1", "ps_comment", "LZ4SIMDBP128"},
      {"UNACCESSED", "region_tpch_1", "r_comment", "LZ4SIMDBP128"},
      {"UNACCESSED", "region_tpch_0_1", "r_comment", "LZ4SIMDBP128"},
      {"ACCESSED", "customer_tpch_1", "c_comment", "DictionaryFSBA"},
      {"ACCESSED", "customer_tpch_0_1", "c_comment", "DictionaryFSBA"},
      {"ACCESSED", "customer_tpch_1", "c_address", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_0_1", "c_address", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_1", "c_phone", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_0_1", "c_phone", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_1", "c_name", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_0_1", "c_name", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_commitdate", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_commitdate", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_receiptdate", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_receiptdate", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_extendedprice", "DictionaryFSBA"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_extendedprice", "DictionaryFSBA"},
      {"ACCESSED", "lineitem_tpch_1", "l_shipdate", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_shipdate", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_orderkey", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_orderkey", "DictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_1", "o_orderkey", "DictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_0_1", "o_orderkey", "DictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_1", "p_partkey", "DictionaryFSBA"},
      {"ACCESSED", "part_tpch_0_1", "p_partkey", "DictionaryFSBA"},
      {"ACCESSED", "customer_tpch_1", "c_custkey", "DictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_0_1", "c_custkey", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_suppkey", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_suppkey", "DictionarySIMDBP128"},
      {"ACCESSED", "partsupp_tpch_1", "ps_suppkey", "DictionarySIMDBP128"},
      {"ACCESSED", "partsupp_tpch_0_1", "ps_suppkey", "DictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_1", "c_nationkey", "DictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_0_1", "c_nationkey", "DictionarySIMDBP128"},
      {"ACCESSED", "partsupp_tpch_1", "ps_partkey", "DictionarySIMDBP128"},
      {"ACCESSED", "partsupp_tpch_0_1", "ps_partkey", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_partkey", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_partkey", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_discount", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_discount", "DictionarySIMDBP128"},
      {"ACCESSED", "partsupp_tpch_1", "ps_supplycost", "DictionarySIMDBP128"},
      {"ACCESSED", "partsupp_tpch_0_1", "ps_supplycost", "DictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_1", "p_name", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_0_1", "p_name", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_1", "o_custkey", "DictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_0_1", "o_custkey", "DictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_1", "c_acctbal", "DictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_0_1", "c_acctbal", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_quantity", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_quantity", "DictionarySIMDBP128"},
      {"ACCESSED", "supplier_tpch_1", "s_comment", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "supplier_tpch_0_1", "s_comment", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "supplier_tpch_1", "s_phone", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "supplier_tpch_0_1", "s_phone", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "partsupp_tpch_1", "ps_availqty", "DictionarySIMDBP128"},
      {"ACCESSED", "partsupp_tpch_0_1", "ps_availqty", "DictionarySIMDBP128"},
      {"ACCESSED", "supplier_tpch_1", "s_address", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "supplier_tpch_0_1", "s_address", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_1", "o_comment", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_0_1", "o_comment", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "supplier_tpch_1", "s_suppkey", "DictionarySIMDBP128"},
      {"ACCESSED", "supplier_tpch_0_1", "s_suppkey", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_tax", "DictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_tax", "DictionarySIMDBP128"},
      {"ACCESSED", "supplier_tpch_1", "s_name", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "supplier_tpch_0_1", "s_name", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_1", "o_totalprice", "DictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_0_1", "o_totalprice", "DictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_1", "o_shippriority", "DictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_0_1", "o_shippriority", "DictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_1", "p_size", "DictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_0_1", "p_size", "DictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_1", "o_orderdate", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_0_1", "o_orderdate", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "nation_tpch_1", "n_nationkey", "FrameOfReferenceSIMDBP128"},
      {"ACCESSED", "nation_tpch_0_1", "n_nationkey", "FrameOfReferenceSIMDBP128"},
      {"ACCESSED", "part_tpch_1", "p_type", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_0_1", "p_type", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "nation_tpch_1", "n_name", "LZ4SIMDBP128"},
      {"ACCESSED", "nation_tpch_0_1", "n_name", "LZ4SIMDBP128"},
      {"ACCESSED", "part_tpch_1", "p_container", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_0_1", "p_container", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_shipmode", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_shipmode", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_returnflag", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_returnflag", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_1", "p_brand", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_0_1", "p_brand", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_1", "l_linestatus", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "lineitem_tpch_0_1", "l_linestatus", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "nation_tpch_1", "n_regionkey", "FrameOfReferenceSIMDBP128"},
      {"ACCESSED", "nation_tpch_0_1", "n_regionkey", "FrameOfReferenceSIMDBP128"},
      {"ACCESSED", "region_tpch_1", "r_regionkey", "FrameOfReferenceSIMDBP128"},
      {"ACCESSED", "region_tpch_0_1", "r_regionkey", "FrameOfReferenceSIMDBP128"},
      {"ACCESSED", "region_tpch_1", "r_name", "DictionarySIMDBP128"},
      {"ACCESSED", "region_tpch_0_1", "r_name", "DictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_1", "o_orderstatus", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_0_1", "o_orderstatus", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_1", "p_mfgr", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "part_tpch_0_1", "p_mfgr", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_1", "c_mktsegment", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "customer_tpch_0_1", "c_mktsegment", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_1", "o_orderpriority", "FixedStringDictionarySIMDBP128"},
      {"ACCESSED", "orders_tpch_0_1", "o_orderpriority", "FixedStringDictionarySIMDBP128"},
  };
};

}  // namespace opossum
