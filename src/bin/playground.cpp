#include <chrono>
#include <iostream>
#include <unordered_map>
#include <random>
#include <optional>
#include <thread>
#include <uWS/uWS.h>

#include "json.hpp"

#include "sql/gds_cache.hpp"
#include "sql/gdfs_cache.hpp"
#include "sql/lru_cache.hpp"
#include "sql/lru_k_cache.hpp"
#include "sql/random_cache.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "utils/thread_pool.h"


using namespace std::chrono_literals;

using CacheKeyType = int;
using CacheValueType = std::string;

int main() {
    constexpr size_t EXECUTIONS = 1000;
    constexpr size_t CACHE_SIZE = 6;

    ThreadPool thread_pool(2);

    std::array<std::string, 15> queries;
    queries[0] = "SELECT * FROM a;";
    queries[1] = "SELECT * FROM b;";
    queries[2] = "SELECT * FROM c;";
    queries[3] = "SELECT * FROM d;";
    queries[4] = "SELECT * FROM e;";
    queries[5] = "SELECT * FROM f;";
    queries[6] = "SELECT * FROM g;";
    queries[7] = "SELECT * FROM h;";
    queries[8] = "SELECT * FROM i;";
    queries[9] = "SELECT * FROM j;";
    queries[10] = "SELECT * FROM k;";
    queries[11] = "SELECT * FROM l;";
    queries[12] = "SELECT * FROM m;";
    queries[13] = "SELECT * FROM n;";
    queries[14] = "SELECT * FROM o;";
    queries[15] = "SELECT * FROM p;";

    std::map<std::string, std::shared_ptr<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>> caches;
    caches.emplace("GDS", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["GDS"]->replace_cache_impl<opossum::GDSCache<CacheKeyType, CacheValueType>>(CACHE_SIZE);

    caches.emplace("GDFS", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["GDFS"]->replace_cache_impl<opossum::GDFSCache<CacheKeyType, CacheValueType>>(CACHE_SIZE);

    caches.emplace("LRU", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU"]->replace_cache_impl<opossum::LRUCache<CacheKeyType, CacheValueType>>(CACHE_SIZE);

    caches.emplace("LRU_K", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["LRU_K"]->replace_cache_impl<opossum::LRUKCache<2, CacheKeyType, CacheValueType>>(CACHE_SIZE);

    caches.emplace("RANDOM", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(CACHE_SIZE));
    caches["RANDOM"]->replace_cache_impl<opossum::RandomCache<CacheKeyType, CacheValueType>>(CACHE_SIZE);


    uWS::Hub h;

    h.onMessage([&caches, &queries, &thread_pool](uWS::WebSocket<uWS::SERVER> *ws, char *message, size_t length, uWS::OpCode opCode) {
        std::cout << "start" << std::endl;


        std::mt19937 rng;
        rng.seed(std::random_device()());
        std::uniform_int_distribution<std::mt19937::result_type> dist(0, queries.size() - 1);


        for (size_t execution = 0; execution < EXECUTIONS; ++execution) {
            nlohmann::json results;
            size_t query_id = dist(rng);

            results["executionId"] = execution;
            results["queryId"] = query_id;
            results["cacheHits"] = {};
            for (auto &[strategy, cache] : caches) {
                std::optional<CacheValueType> cached_plan = cache->try_get(query_id);
                std::optional<CacheKeyType> evicted;
                if (cached_plan) {
                    cache_hit = true;
                } else {
                    results["cacheHits"][strategy] = false;
                    results["planningTime"][strategy] = query.planning_time;
                    evicted = cache->set(query_id, query.sql_string);
                }

                results["evictedQuery"][strategy] = evicted ? *evicted : -1;
            }
            nlohmann::json package;
            package["message"] = "query_execution";
            package["data"] = results;


            auto package_dump = package.dump();
            ws->send(package_dump.c_str());
        } else if (message_json["message"] == "update_config") {
            size_t cache_size = message_json["data"]["cacheSize"];

            caches.clear();

            caches.emplace("GDS", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(cache_size));
            caches["GDS"]->replace_cache_impl<opossum::GDSCache<CacheKeyType, CacheValueType>>(cache_size);

            caches.emplace("GDFS", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(cache_size));
            caches["GDFS"]->replace_cache_impl<opossum::GDFSCache<CacheKeyType, CacheValueType>>(cache_size);

            caches.emplace("LRU", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(cache_size));
            caches["LRU"]->replace_cache_impl<opossum::LRUCache<CacheKeyType, CacheValueType>>(cache_size);

            caches.emplace("LRU_K", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(cache_size));
            caches["LRU_K"]->replace_cache_impl<opossum::LRUKCache<2, CacheKeyType, CacheValueType>>(cache_size);

            caches.emplace("RANDOM", std::make_shared<opossum::SQLQueryCache<CacheValueType, CacheKeyType>>(cache_size));
            caches["RANDOM"]->replace_cache_impl<opossum::RandomCache<CacheKeyType, CacheValueType>>(cache_size);

            std::cout << "Cache size set to " << cache_size << std::endl;
        }
    });

    if (h.listen(4000)) {
        h.run();
    }
}
