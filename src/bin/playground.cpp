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

int main() {
    constexpr size_t CACHE_SIZE = 30;
    size_t execution_id = 1;

    std::map<std::string, std::shared_ptr<opossum::SQLQueryCache<std::string>>> caches;
    caches.emplace("GDS", std::make_shared<opossum::SQLQueryCache<std::string>>(CACHE_SIZE));
    caches["GDS"]->replace_cache_impl<opossum::GDSCache<std::string, std::string>>(CACHE_SIZE);

    caches.emplace("GDFS", std::make_shared<opossum::SQLQueryCache<std::string>>(CACHE_SIZE));
    caches["GDFS"]->replace_cache_impl<opossum::GDFSCache<std::string, std::string>>(CACHE_SIZE);

    caches.emplace("LRU", std::make_shared<opossum::SQLQueryCache<std::string>>(CACHE_SIZE));
    caches["LRU"]->replace_cache_impl<opossum::LRUCache<std::string, std::string>>(CACHE_SIZE);

    caches.emplace("LRU_K", std::make_shared<opossum::SQLQueryCache<std::string>>(CACHE_SIZE));
    caches["LRU_K"]->replace_cache_impl<opossum::LRUKCache<2, std::string, std::string>>(CACHE_SIZE);

    caches.emplace("RANDOM", std::make_shared<opossum::SQLQueryCache<std::string>>(CACHE_SIZE));
    caches["RANDOM"]->replace_cache_impl<opossum::RandomCache<std::string, std::string>>(CACHE_SIZE);

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

    h.onMessage([&caches, &workloads, &execution_id](uWS::WebSocket<uWS::SERVER> *ws, char *message, size_t length, uWS::OpCode opCode) {
        auto message_json = nlohmann::json::parse(std::string(message, length));
        if (message_json["message"] == "execute_query") {
            auto workload_id = message_json["data"]["workload"];
            auto query_id = message_json["data"]["query"];

            std::cout << "Execute query: " << query_id << " from workload: " << workload_id << std::endl;

            auto& query = workloads[workload_id][query_id];

            nlohmann::json results;

            results["executionId"] = execution_id++;
            results["workload"] = workload_id;
            results["query"] = query_id;
            results["cacheHits"] = {};
            results["planningTime"] = {};
            for (auto &[strategy, cache] : caches) {
                std::optional<std::string> cached_plan = cache->try_get(query.sql_string);
                if (cached_plan) {
                    results["cacheHits"][strategy] = true;
                    results["planningTime"][strategy] = 0.0f;
                } else {
                    results["cacheHits"][strategy] = false;
                    results["planningTime"][strategy] = query.planning_time;
                    cache->set(query.sql_string, query.sql_string);
                }
            }
            nlohmann::json package;
            package["message"] = "query_execution";
            package["data"] = results;


            auto package_dump = package.dump();
            ws->send(package_dump.c_str());
        } else if (message_json["message"] == "update_config") {
            size_t cache_size = message_json["data"]["cacheSize"];

            caches.clear();

            caches.emplace("GDS", std::make_shared<opossum::SQLQueryCache<std::string>>(cache_size));
            caches["GDS"]->replace_cache_impl<opossum::GDSCache<std::string, std::string>>(cache_size);

            caches.emplace("GDFS", std::make_shared<opossum::SQLQueryCache<std::string>>(cache_size));
            caches["GDFS"]->replace_cache_impl<opossum::GDFSCache<std::string, std::string>>(cache_size);

            caches.emplace("LRU", std::make_shared<opossum::SQLQueryCache<std::string>>(cache_size));
            caches["LRU"]->replace_cache_impl<opossum::LRUCache<std::string, std::string>>(cache_size);

            caches.emplace("LRU_K", std::make_shared<opossum::SQLQueryCache<std::string>>(cache_size));
            caches["LRU_K"]->replace_cache_impl<opossum::LRUKCache<2, std::string, std::string>>(cache_size);

            caches.emplace("RANDOM", std::make_shared<opossum::SQLQueryCache<std::string>>(cache_size));
            caches["RANDOM"]->replace_cache_impl<opossum::RandomCache<std::string, std::string>>(cache_size);

            std::cout << "Cache size set to " << cache_size << std::endl;
        }

    });

    if (h.listen(4000)) {
        h.run();
    }

    std::cout << "out" << std::endl;
}
