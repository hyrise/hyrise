#include <iostream>
#include <vector>
#include <uWS/uWS.h>

#include "sql/gdfs_cache.hpp"
#include "sql/lru_cache.hpp"
#include "sql/lru_k_cache.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"


int main() {
    uWS::Hub h;

    h.onMessage([](uWS::WebSocket<uWS::SERVER> *ws, char *message, size_t length, uWS::OpCode opCode) {
        ws->send(message, length, opCode);
    });

    if (h.listen(3000)) {
        h.run();
    }

    // std::vector<std::shared_ptr<opossum::SQLQueryCache<opossum::SQLQueryPlan>>> caches;
    // caches.emplace_back(std::make_shared<opossum::SQLQueryCache<opossum::SQLQueryPlan>>(1024, opossum::GDFS));
}
