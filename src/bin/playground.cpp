#include <iostream>
#include <map>

#include "../benchmarklib/benchmark_sql_executor.hpp"
#include "hyrise.hpp"
#include "operators/import.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/timer.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  // DS 82, 85, 26
  // JOB 13a, 28a, 22c

  const auto ds_tables = std::vector<std::string>{"catalog_sales",    "customer_demographics",
                                                  "date_dim",         "item",
                                                  "promotion",        "inventory",
                                                  "store_sales",      "web_sales",
                                                  "web_returns",      "web_page",
                                                  "customer_address", "reason"};

  const auto job_tables = std::vector<std::string>{
      "company_name",   "company_type", "info_type", "kind_type",     "movie_companies", "movie_info",
      "movie_info_idx", "title",        "keyword",   "movie_keyword", "complete_cast",   "comp_cast_type"};

  const auto initial_scheduler = Hyrise::get().scheduler();
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  const auto ds_path = std::string{"tpcds_cached_tables/sf-10/"};
  const auto job_path = std::string{"imdb_data/"};

  const auto plugin_path = "cmake-build-release/lib/libhyriseDependencyDiscoveryPlugin.so";
  const auto num_repetitions = 100;

  const auto import_tables = [](const auto& path, const auto& table_names) {
    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
    tasks.reserve(table_names.size());
    for (const auto& table_name : table_names) {
      tasks.push_back(std::make_shared<JobTask>([&path, table_name]() {
        const auto importer = std::make_shared<Import>(path + table_name + ".bin", table_name);
        importer->execute();
        std::cout << " - imported " << table_name << std::endl;
      }));
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  };

  std::cout << "- Import tables" << std::endl;

  import_tables(ds_path, ds_tables);
  import_tables(job_path, job_tables);

  Hyrise::get().set_scheduler(initial_scheduler);

  const auto regular = std::map<std::string, std::string>{
      {"DS-26",
       R"(SELECT i_item_id, avg(cs_quantity) agg1, avg(cs_list_price) agg2, avg(cs_coupon_amt) agg3, avg(cs_sales_price) agg4 FROM catalog_sales, customer_demographics, date_dim, item, promotion WHERE cs_sold_date_sk = d_date_sk AND cs_item_sk = i_item_sk AND cs_bill_cdemo_sk = cd_demo_sk AND cs_promo_sk = p_promo_sk AND cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'College' AND (p_channel_email = 'N' OR p_channel_event = 'N') AND d_year = 2000 GROUP BY i_item_id ORDER BY i_item_id LIMIT 100;)"},
      {"DS-37",
       R"(SELECT i_item_id, i_item_desc, i_current_price FROM item, inventory, date_dim, catalog_sales WHERE i_current_price BETWEEN 68 AND 68 + 30 AND inv_item_sk = i_item_sk AND d_date_sk=inv_date_sk AND d_date BETWEEN cast('2000-02-01' AS date) AND cast('2000-04-01' AS date) AND i_manufact_id IN (677, 940, 694, 808) AND inv_quantity_on_hand BETWEEN 100 AND 500 AND cs_item_sk = i_item_sk GROUP BY i_item_id, i_item_desc, i_current_price ORDER BY i_item_id LIMIT 100;)"},
      {"DS-82",
       R"(SELECT i_item_id , i_item_desc , i_current_price FROM item, inventory, date_dim, store_sales WHERE i_current_price BETWEEN 62 AND 62+30 AND inv_item_sk = i_item_sk AND d_date_sk=inv_date_sk AND d_date BETWEEN cast('2000-05-25' AS date) AND cast('2000-07-24' AS date) AND i_manufact_id IN (129, 270, 821, 423) AND inv_quantity_on_hand BETWEEN 100 AND 500 AND ss_item_sk = i_item_sk GROUP BY i_item_id, i_item_desc, i_current_price ORDER BY i_item_id LIMIT 100;)"},
      {"DS-85",
       R"(SELECT SUBSTR(r_reason_desc,1,20) , avg(ws_quantity) , avg(wr_refunded_cash) , avg(wr_fee) FROM web_sales, web_returns, web_page, customer_demographics cd1, customer_demographics cd2, customer_address, date_dim, reason WHERE ws_web_page_sk = wp_web_page_sk AND ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number AND ws_sold_date_sk = d_date_sk AND d_year = 2000 AND cd1.cd_demo_sk = wr_refunded_cdemo_sk AND cd2.cd_demo_sk = wr_returning_cdemo_sk AND ca_address_sk = wr_refunded_addr_sk AND r_reason_sk = wr_reason_sk AND ( ( cd1.cd_marital_status = 'M' AND cd1.cd_marital_status = cd2.cd_marital_status AND cd1.cd_education_status = 'Advanced Degree' AND cd1.cd_education_status = cd2.cd_education_status AND ws_sales_price BETWEEN 100.00 AND 150.00 ) OR ( cd1.cd_marital_status = 'S' AND cd1.cd_marital_status = cd2.cd_marital_status AND cd1.cd_education_status = 'College' AND cd1.cd_education_status = cd2.cd_education_status AND ws_sales_price BETWEEN 50.00 AND 100.00 ) OR ( cd1.cd_marital_status = 'W' AND cd1.cd_marital_status = cd2.cd_marital_status AND cd1.cd_education_status = '2 yr Degree' AND cd1.cd_education_status = cd2.cd_education_status AND ws_sales_price BETWEEN 150.00 AND 200.00 ) ) AND ( ( ca_country = 'United States' AND ca_state IN ('IN', 'OH', 'NJ') AND ws_net_profit BETWEEN 100 AND 200) OR ( ca_country = 'United States' AND ca_state IN ('WI', 'CT', 'KY') AND ws_net_profit BETWEEN 150 AND 300) OR ( ca_country = 'United States' AND ca_state IN ('LA', 'IA', 'AR') AND ws_net_profit BETWEEN 50 AND 250) ) GROUP BY r_reason_desc ORDER BY SUBSTR(r_reason_desc,1,20) , avg(ws_quantity) , avg(wr_refunded_cash) , avg(wr_fee) LIMIT 100;)"},

      {"JOB-13a",
       R"(SELECT MIN(mi.info) AS release_date, MIN(miidx.info) AS rating, MIN(t.title) AS german_movie FROM company_name AS cn, company_type AS ct, info_type AS it, info_type AS it2, kind_type AS kt, movie_companies AS mc, movie_info AS mi, movie_info_idx AS miidx, title AS t WHERE cn.country_code ='[de]' AND ct.kind ='production companies' AND it.info ='rating' AND it2.info ='release dates' AND kt.kind ='movie' AND mi.movie_id = t.id AND it2.id = mi.info_type_id AND kt.id = t.kind_id AND mc.movie_id = t.id AND cn.id = mc.company_id AND ct.id = mc.company_type_id AND miidx.movie_id = t.id AND it.id = miidx.info_type_id AND mi.movie_id = miidx.movie_id AND mi.movie_id = mc.movie_id AND miidx.movie_id = mc.movie_id;)"},
      {"JOB-22c",
       R"(SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS western_violent_movie FROM company_name AS cn, company_type AS ct, info_type AS it1, info_type AS it2, keyword AS k, kind_type AS kt, movie_companies AS mc, movie_info AS mi, movie_info_idx AS mi_idx, movie_keyword AS mk, title AS t WHERE cn.country_code != '[us]' AND it1.info = 'countries' AND it2.info = 'rating' AND k.keyword IN ('murder', 'murder-in-title', 'blood', 'violence') AND kt.kind IN ('movie', 'episode') AND mc.note NOT LIKE '%(USA)%' AND mc.note LIKE '%(200%)%' AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American') AND mi_idx.info < '8.5' AND t.production_year > 2005 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mi_idx.movie_id AND t.id = mc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mi_idx.movie_id AND mk.movie_id = mc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mc.movie_id AND mc.movie_id = mi_idx.movie_id AND k.id = mk.keyword_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id AND ct.id = mc.company_type_id AND cn.id = mc.company_id;)"},
      {"JOB-28a",
       R"(SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_euro_dark_movie FROM complete_cast AS cc, comp_cast_type AS cct1, comp_cast_type AS cct2, company_name AS cn, company_type AS ct, info_type AS it1, info_type AS it2, keyword AS k, kind_type AS kt, movie_companies AS mc, movie_info AS mi, movie_info_idx AS mi_idx, movie_keyword AS mk, title AS t WHERE cct1.kind = 'crew' AND cct2.kind != 'complete+verified' AND cn.country_code != '[us]' AND it1.info = 'countries' AND it2.info = 'rating' AND k.keyword IN ('murder', 'murder-in-title', 'blood', 'violence') AND kt.kind IN ('movie', 'episode') AND mc.note NOT LIKE '%(USA)%' AND mc.note LIKE '%(200%)%' AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American') AND mi_idx.info < '8.5' AND t.production_year > 2000 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mi_idx.movie_id AND t.id = mc.movie_id AND t.id = cc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mi_idx.movie_id AND mk.movie_id = mc.movie_id AND mk.movie_id = cc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mc.movie_id AND mi.movie_id = cc.movie_id AND mc.movie_id = mi_idx.movie_id AND mc.movie_id = cc.movie_id AND mi_idx.movie_id = cc.movie_id AND k.id = mk.keyword_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id AND ct.id = mc.company_type_id AND cn.id = mc.company_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id;)"}};

  const auto rewritten = std::map<std::string,
                                  std::string>{{"DS-26",
                                                R"(select min(d_date_sk), max(d_date_sk) from date_dim where d_year = 2000; SELECT i_item_id, avg(cs_quantity) agg1, avg(cs_list_price) agg2, avg(cs_coupon_amt) agg3, avg(cs_sales_price) agg4 FROM catalog_sales, customer_demographics, item, promotion WHERE cs_sold_date_sk BETWEEN 2451545 AND 2451910 AND cs_item_sk = i_item_sk AND cs_bill_cdemo_sk = cd_demo_sk AND cs_promo_sk = p_promo_sk AND cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'College' AND (p_channel_email = 'N' OR p_channel_event = 'N') GROUP BY i_item_id ORDER BY i_item_id LIMIT 100;)"},
                                               {"DS-82",
                                                R"(select min(d_date_sk), max(d_date_sk) from date_dim where d_date BETWEEN cast('2000-05-25' AS date) AND cast('2000-07-24' AS date); SELECT i_item_id , i_item_desc , i_current_price FROM item, inventory, store_sales WHERE i_current_price BETWEEN 62 AND 62+30 AND inv_item_sk = i_item_sk AND  inv_date_sk BETWEEN 2451690 AND 2451750 AND i_manufact_id IN (129, 270, 821, 423) AND inv_quantity_on_hand BETWEEN 100 AND 500 AND ss_item_sk = i_item_sk GROUP BY i_item_id, i_item_desc, i_current_price ORDER BY i_item_id LIMIT 100;)"},
                                               {"DS-37", R"(select min(d_date_sk), max(d_date_sk) from date_dim where d_date BETWEEN cast('2000-02-01' AS date) AND cast('2000-04-01' AS date); SELECT i_item_id, i_item_desc, i_current_price FROM item, inventory, catalog_sales WHERE i_current_price BETWEEN 68 AND 68 + 30 AND inv_item_sk = i_item_sk AND  inv_date_sk BETWEEN 2451576 AND 2451636 AND i_manufact_id IN (677, 940, 694, 808) AND inv_quantity_on_hand BETWEEN 100 AND 500 AND cs_item_sk = i_item_sk GROUP BY i_item_id, i_item_desc, i_current_price ORDER BY i_item_id LIMIT 100;)"},
                                               {"DS-85",
                                                R"(select min(d_date_sk), max(d_date_sk) from date_dim where d_year = 2000; SELECT SUBSTR(r_reason_desc,1,20) , avg(ws_quantity) , avg(wr_refunded_cash) , avg(wr_fee) FROM web_sales, web_returns, web_page, customer_demographics cd1, customer_demographics cd2, customer_address, reason WHERE ws_web_page_sk = wp_web_page_sk AND ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number AND ws_sold_date_sk BETWEEN 2451545 AND 2451910 AND cd1.cd_demo_sk = wr_refunded_cdemo_sk AND cd2.cd_demo_sk = wr_returning_cdemo_sk AND ca_address_sk = wr_refunded_addr_sk AND r_reason_sk = wr_reason_sk AND ( ( cd1.cd_marital_status = 'M' AND cd1.cd_marital_status = cd2.cd_marital_status AND cd1.cd_education_status = 'Advanced Degree' AND cd1.cd_education_status = cd2.cd_education_status AND ws_sales_price BETWEEN 100.00 AND 150.00 ) OR ( cd1.cd_marital_status = 'S' AND cd1.cd_marital_status = cd2.cd_marital_status AND cd1.cd_education_status = 'College' AND cd1.cd_education_status = cd2.cd_education_status AND ws_sales_price BETWEEN 50.00 AND 100.00 ) OR ( cd1.cd_marital_status = 'W' AND cd1.cd_marital_status = cd2.cd_marital_status AND cd1.cd_education_status = '2 yr Degree' AND cd1.cd_education_status = cd2.cd_education_status AND ws_sales_price BETWEEN 150.00 AND 200.00 ) ) AND ( ( ca_country = 'United States' AND ca_state IN ('IN', 'OH', 'NJ') AND ws_net_profit BETWEEN 100 AND 200) OR ( ca_country = 'United States' AND ca_state IN ('WI', 'CT', 'KY') AND ws_net_profit BETWEEN 150 AND 300) OR ( ca_country = 'United States' AND ca_state IN ('LA', 'IA', 'AR') AND ws_net_profit BETWEEN 50 AND 250) ) GROUP BY r_reason_desc ORDER BY SUBSTR(r_reason_desc,1,20) , avg(ws_quantity) , avg(wr_refunded_cash) , avg(wr_fee) LIMIT 100;)"},

                                               {"JOB-13a",
                                                R"(select id from company_type where kind = 'production companies'; select id from info_type where info = 'release dates'; select id from info_type where info = 'rating'; select id from kind_type where kind = 'movie'; SELECT MIN(mi.info) AS release_date, MIN(miidx.info) AS rating, MIN(t.title) AS german_movie FROM company_name AS cn, movie_companies AS mc, movie_info AS mi, movie_info_idx AS miidx, title AS t WHERE cn.country_code ='[de]' AND mi.movie_id = t.id AND mi.info_type_id = 16 AND t.kind_id = 1 AND mc.movie_id = t.id AND cn.id = mc.company_id AND mc.company_type_id = 2 AND miidx.movie_id = t.id AND miidx.info_type_id = 101 AND mi.movie_id = miidx.movie_id AND mi.movie_id = mc.movie_id AND miidx.movie_id = mc.movie_id;)"},
                                               {"JOB-22c",
                                                R"(select id from info_type where info = 'countries'; select id from info_type where info = 'rating'; SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS western_violent_movie FROM company_name AS cn, company_type AS ct, keyword AS k, kind_type AS kt, movie_companies AS mc, movie_info AS mi, movie_info_idx AS mi_idx, movie_keyword AS mk, title AS t WHERE cn.country_code != '[us]' AND k.keyword IN ('murder', 'murder-in-title', 'blood', 'violence') AND kt.kind IN ('movie', 'episode') AND mc.note NOT LIKE '%(USA)%' AND mc.note LIKE '%(200%)%' AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American') AND mi_idx.info < '8.5' AND t.production_year > 2005 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mi_idx.movie_id AND t.id = mc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mi_idx.movie_id AND mk.movie_id = mc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mc.movie_id AND mc.movie_id = mi_idx.movie_id AND k.id = mk.keyword_id AND mi.info_type_id = 8 AND mi_idx.info_type_id = 101 AND ct.id = mc.company_type_id AND cn.id = mc.company_id;)"},
                                               {"JOB-28a",
                                                R"(select id from info_type where info = 'countries'; select id from info_type where info = 'rating'; select id from comp_cast_type where kind = 'crew'; SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_euro_dark_movie FROM complete_cast AS cc, comp_cast_type AS cct2, company_name AS cn, company_type AS ct, keyword AS k, kind_type AS kt, movie_companies AS mc, movie_info AS mi, movie_info_idx AS mi_idx, movie_keyword AS mk, title AS t WHERE cct2.kind != 'complete+verified' AND cn.country_code != '[us]' AND k.keyword IN ('murder', 'murder-in-title', 'blood', 'violence') AND kt.kind IN ('movie', 'episode') AND mc.note NOT LIKE '%(USA)%' AND mc.note LIKE '%(200%)%' AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American') AND mi_idx.info < '8.5' AND t.production_year > 2000 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mi_idx.movie_id AND t.id = mc.movie_id AND t.id = cc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mi_idx.movie_id AND mk.movie_id = mc.movie_id AND mk.movie_id = cc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mc.movie_id AND mi.movie_id = cc.movie_id AND mc.movie_id = mi_idx.movie_id AND mc.movie_id = cc.movie_id AND mi_idx.movie_id = cc.movie_id AND k.id = mk.keyword_id AND mi.info_type_id = 8 AND mi_idx.info_type_id = 101 AND ct.id = mc.company_type_id AND cn.id = mc.company_id AND cc.subject_id = 2 AND cct2.id = cc.status_id;)"}};

  const auto execute_queries = [&](const auto& queries) {
    auto executor = BenchmarkSQLExecutor{nullptr, std::nullopt};

    for (const auto& [id, query] : queries) {
      std::cout << "\"" << id << "\": " << std::flush;
      auto run_sum = std::chrono::nanoseconds{0};
      // warmup
      const auto& [w_status, _] = executor.execute(query);
      Assert(w_status == SQLPipelineStatus::Success, "wtf");

      for (auto repetition = 0; repetition < num_repetitions; ++repetition) {
        auto timer = Timer{};
        const auto& [status, _] = executor.execute(query);
        run_sum += timer.lap();
        Assert(status == SQLPipelineStatus::Success, "wtf");
      }
      const auto avg_time = static_cast<double>(run_sum.count()) / static_cast<double>(num_repetitions);
      std::cout << std::fixed << avg_time << "," << std::endl;
    }
  };

  auto pwd = PerformanceWarningDisabler{};

  std::cout << std::endl << "=================\n REWRITTEN\n=================" << std::endl;
  execute_queries(rewritten);

  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  std::cout << std::endl << "=================\n REGULAR\n=================" << std::endl;
  execute_queries(regular);

  std::cout << std::endl << std::endl;
  auto& plugin_manager = Hyrise::get().plugin_manager;

  plugin_manager.load_plugin(plugin_path);
  plugin_manager.exec_user_function("hyriseDependencyDiscoveryPlugin", "DiscoverDependencies");

  std::cout << std::endl << "=================\n OPTIMIZED\n=================" << std::endl;
  execute_queries(regular);

  plugin_manager.unload_plugin("hyriseDependencyDiscoveryPlugin");

  return 0;
}
