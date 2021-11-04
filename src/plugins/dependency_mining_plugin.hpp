#pragma once

#include "dependency_mining/dependency_validator.hpp"
#include "dependency_mining/pqp_analyzer.hpp"
#include "dependency_mining/util.hpp"
#include "utils/abstract_plugin.hpp"

/* Dependency Mining / Optimization doc
 *
 *  for enabling specific optimizations and dependency mining: - check switches at plugins/dependency_mining/dependency_usage_config.hpp
 *  when mining plugins/to turn off all optimizations: - comment out benchmarklib/abstract_table_generator.cpp L188 _add_constraints(table_info_by_name);
 *      --> otherwise, UCC mining will notice that dependency is already set and do early-out
 *  plugin is loaded by using CLI option --dep_mining_plugin <plugin_path>
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
