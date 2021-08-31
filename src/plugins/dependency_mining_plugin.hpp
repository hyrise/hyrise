#pragma once

#include "dependency_mining/dependency_validator.hpp"
#include "dependency_mining/pqp_analyzer.hpp"
#include "dependency_mining/util.hpp"
#include "utils/abstract_plugin.hpp"

/* Dependency Mining / Optimization doc
 *
 *  for enabling specific optimizations: - check switches at lib/optimizer/strategy/column_pruning_rule.hpp for Join2Semi and Join2Predciate
 *                                       - consider commenting out DependentGroupByReductionRule at lib/optimizer/optimizer.cpp L57
 *  for mining dependencies for specific optimizations: - check switches at plugins/dependency_mining/pqp_analyzer.hpp
 *  when mining plugins/to turn off all optimizations: - comment out benchmarklib/abstract_table_generator.cpp L188 _add_constraints(table_info_by_name);
 *      --> otherwise, UCC mining will notice that dependency is already set and do early-out
 *  extra TPC-DS queries are shipped at <project_root>/tpcds_extra_queries
 *  plugin is loaded at benchmarklib/benchmark_runner.cpp L182 --> make sure so set correct path there
 */
namespace opossum {

class DependencyMiningPlugin : public AbstractPlugin {
 public:
  explicit DependencyMiningPlugin();
  std::string description() const final;

  void start() final;

  void stop() final;

  constexpr static size_t NUM_VALIDATORS = 1;
  constexpr static bool DO_VALIDATE = true;

 protected:
  std::shared_ptr<DependencyCandidateQueue> _queue;
  PQPAnalyzer _pqp_analyzer;
  std::vector<std::unique_ptr<DependencyValidator>> _dependency_validators;
};

}  // namespace opossum
