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
    nlohmann::json results;

    results["executionId"] = execution;
    results["queryId"] = query_id;
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
}

int main() {

    // ThreadPool thread_pool(2);


    uWS::Hub h;

    h.onConnection([](uWS::WebSocket<uWS::SERVER> *ws, uWS::HttpRequest req) {
        std::cout << "Connected!" << std::endl;
        auto workloads = initialize_workloads();

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
        std::cout << initial_data_dump << std::endl;
        ws->send(initial_data_dump.c_str());

    });

    // h.onMessage([](uWS::WebSocket<uWS::SERVER> *ws, char *message, size_t length, uWS::OpCode opCode) {
    //     std::cout << "start" << std::endl;

    //     constexpr size_t EXECUTIONS = 1000;
    //     constexpr size_t CACHE_SIZE = 6;

    //     // auto queries = load_queries();

    //     std::mt19937 rng;
    //     rng.seed(std::random_device()());
    //     std::uniform_int_distribution<std::mt19937::result_type> dist(0, queries.size() - 1);

    //     std::map<std::string, std::shared_ptr<opossum::SQLQueryCache<std::string>>> caches;

    //     caches.emplace("GDS", std::make_shared<opossum::SQLQueryCache<std::string>>(CACHE_SIZE));
    //     caches["GDS"]->replace_cache_impl<opossum::GDSCache<std::string, std::string>>(CACHE_SIZE);

    //     caches.emplace("GDFS", std::make_shared<opossum::SQLQueryCache<std::string>>(CACHE_SIZE));
    //     caches["GDFS"]->replace_cache_impl<opossum::GDFSCache<std::string, std::string>>(CACHE_SIZE);

    //     caches.emplace("LRU", std::make_shared<opossum::SQLQueryCache<std::string>>(CACHE_SIZE));
    //     caches["LRU"]->replace_cache_impl<opossum::LRUCache<std::string, std::string>>(CACHE_SIZE);

    //     caches.emplace("LRU_K", std::make_shared<opossum::SQLQueryCache<std::string>>(CACHE_SIZE));
    //     caches["LRU_K"]->replace_cache_impl<opossum::LRUKCache<2, std::string, std::string>>(CACHE_SIZE);

    //     caches.emplace("RANDOM", std::make_shared<opossum::SQLQueryCache<std::string>>(CACHE_SIZE));
    //     caches["RANDOM"]->replace_cache_impl<opossum::RandomCache<std::string, std::string>>(CACHE_SIZE);

    //     for (size_t execution = 0; execution < EXECUTIONS; ++execution) {
    //         size_t query_id = dist(rng);
    //         auto& query = queries[query_id];

    //         std::thread t(evaluate_query, ws, std::ref(query), std::ref(caches), execution, query_id);
    //         std::this_thread::sleep_for(0.2s);
    //         t.join();
    //     }
    // });

    if (h.listen(4000)) {
        h.run();
    }

    std::cout << "out" << std::endl;
}
