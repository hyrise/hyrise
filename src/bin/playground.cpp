#include <chrono>
#include <iostream>
#include <unordered_map>
#include <random>
#include <optional>
#include <thread>
#include <uWS/uWS.h>

#include "json.hpp"

#include "queries.cpp"

#include "sql/gds_cache.hpp"
#include "sql/gdfs_cache.hpp"
#include "sql/lru_cache.hpp"
#include "sql/lru_k_cache.hpp"
#include "sql/random_cache.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "sql/sql_translator.hpp"
#include "utils/thread_pool.h"


using namespace std::chrono_literals;
using hsql::SQLStatement;
using hsql::PrepareStatement;
using hsql::ExecuteStatement;
using hsql::kStmtPrepare;
using hsql::kStmtExecute;
using hsql::SQLParser;
using hsql::SQLParserResult;

void evaluate_query(uWS::WebSocket<uWS::SERVER> *ws, Query& query, std::map<std::string, std::shared_ptr<opossum::SQLQueryCache<std::string>>>& caches, size_t execution, size_t query_id) {
    return;
}

using CacheKeyType = std::string;
using CacheValueType = std::string;

int main() {
    constexpr size_t CACHE_SIZE = 30;
    size_t lru_k_value = 2;
    size_t execution_id = 1;

    std::map<std::string, std::shared_ptr<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>> caches;
    caches.emplace("GDS", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["GDS"]->replace_cache_impl<opossum::GDSCache<CacheKeyType, CacheValueType>>(CACHE_SIZE);

    caches.emplace("GDFS", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["GDFS"]->replace_cache_impl<opossum::GDFSCache<CacheKeyType, CacheValueType>>(CACHE_SIZE);

    caches.emplace("LRU", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU"]->replace_cache_impl<opossum::LRUCache<CacheKeyType, CacheValueType>>(CACHE_SIZE);

    caches.emplace("LRU_2", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU_2"]->replace_cache_impl<opossum::LRUKCache<2, CacheKeyType, CacheValueType>>(CACHE_SIZE);
    caches.emplace("LRU_3", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU_3"]->replace_cache_impl<opossum::LRUKCache<3, CacheKeyType, CacheValueType>>(CACHE_SIZE);
    caches.emplace("LRU_4", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU_4"]->replace_cache_impl<opossum::LRUKCache<4, CacheKeyType, CacheValueType>>(CACHE_SIZE);
    caches.emplace("LRU_5", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU_5"]->replace_cache_impl<opossum::LRUKCache<5, CacheKeyType, CacheValueType>>(CACHE_SIZE);
    caches.emplace("LRU_6", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU_6"]->replace_cache_impl<opossum::LRUKCache<6, CacheKeyType, CacheValueType>>(CACHE_SIZE);
    caches.emplace("LRU_7", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU_7"]->replace_cache_impl<opossum::LRUKCache<7, CacheKeyType, CacheValueType>>(CACHE_SIZE);
    caches.emplace("LRU_8", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU_8"]->replace_cache_impl<opossum::LRUKCache<8, CacheKeyType, CacheValueType>>(CACHE_SIZE);
    caches.emplace("LRU_9", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU_9"]->replace_cache_impl<opossum::LRUKCache<9, CacheKeyType, CacheValueType>>(CACHE_SIZE);
    caches.emplace("LRU_10", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU_10"]->replace_cache_impl<opossum::LRUKCache<10, CacheKeyType, CacheValueType>>(CACHE_SIZE);

    caches.emplace("RANDOM", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["RANDOM"]->replace_cache_impl<opossum::RandomCache<CacheKeyType, CacheValueType>>(CACHE_SIZE);

    auto workloads = initialize_workloads();

    uWS::Hub h;

    h.onConnection([&workloads](uWS::WebSocket<uWS::SERVER> *ws, uWS::HttpRequest req) {
        std::cout << "Connected!" << std::endl;

        nlohmann::json initial_data;
        initial_data["message"]  = "startup";
        initial_data["data"]     =  {};

        nlohmann::json tpch;
        tpch["name"] = "TPC-H";
        tpch["id"] = "tpch";
        tpch["queryCount"] = workloads["tpch"].size();

        nlohmann::json tpcc;
        tpcc["name"] = "TPC-C";
        tpcc["id"] = "tpcc";
        tpcc["queryCount"] = workloads["tpcc"].size();

        nlohmann::json join_order;
        join_order["name"] = "Join Order Benchmark";
        join_order["id"] = "join_order";
        join_order["queryCount"] = workloads["join_order"].size();

        initial_data["data"]["workloads"] = {tpch, tpcc, join_order};

        auto initial_data_dump = initial_data.dump();
        ws->send(initial_data_dump.c_str());

    });

    h.onMessage([&caches, &workloads, &execution_id, &lru_k_value](uWS::WebSocket<uWS::SERVER> *ws, char *message, size_t length, uWS::OpCode opCode) {
        auto message_json = nlohmann::json::parse(std::string(message, length));
        if (message_json["message"] == "execute_query") {
            std::string workload_id = message_json["data"]["workload"];
            size_t query_id = message_json["data"]["query"];
            std::string query_key = workload_id + std::string("__") + std::to_string(query_id);

            std::cout << "Execute query: " << query_id << " from workload: " << workload_id << std::endl;

            auto& query = workloads[workload_id][query_id];

            nlohmann::json results;

            results["executionId"] = execution_id++;
            results["workload"] = workload_id;
            results["query"] = query_id;
            results["cacheHits"] = {};
            results["planningTime"] = {};
            for (auto &[strategy, cache] : caches) {
                std::optional<CacheValueType> cached_plan = cache->try_get(query_key);
                std::optional<CacheKeyType> evicted;
                bool hit;
                float planning_time;
                if (cached_plan) {
                    // std::cout << "Cache Hit: " << query_key << std::endl;
                    hit = true;
                    planning_time = 0.0f;
                } else {
                    // std::cout << "Cache Miss: " << query_key << std::endl;
                    hit = false;
                    planning_time = query.planning_time;
                    evicted = cache->set(query_key, query.sql_string, query.planning_time, query.num_tokens);
                }
                if (strategy.find("LRU_") == std::string::npos || strategy.find(std::to_string(lru_k_value)) != std::string::npos) {
                    auto current_strategy = strategy;
                    if (strategy.find("LRU_") != std::string::npos) {
                        current_strategy = "LRU_K";
                    }

                    results["cacheHits"][current_strategy] = hit;
                    results["planningTime"][current_strategy] = planning_time;
                    results["evictedQuery"][current_strategy] = evicted ? *evicted : "-1";
                }
            }
            nlohmann::json package;
            package["message"] = "query_execution";
            package["data"] = results;


            auto package_dump = package.dump();
            ws->send(package_dump.c_str());
        } else if (message_json["message"] == "update_config") {
            size_t cache_size = message_json["data"]["cacheSize"];

            for (auto &[strategy, cache] : caches) {
                cache->resize(cache_size);
            }
            std::cout << "Cache size set to " << cache_size << std::endl;

            size_t new_k_value = message_json["data"]["lruKValue"];
            if (new_k_value != lru_k_value) {
                lru_k_value = new_k_value;

                std::cout << "LRU's K value set to: " << message_json["data"]["lruKValue"] << std::endl;
            }
        } else if (message_json["message"] == "stop_benchmark") {

            for (auto &[strategy, cache] : caches) {
                cache->clear();
            }

            execution_id = 1;

            std::cout << "Benchmark stopped. Caches are cleared." << std::endl;
        }

    });

    if (h.listen(4000)) {
        h.run();
    }

    std::cout << "out" << std::endl;
}
