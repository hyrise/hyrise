#pragma once

#include <chrono>

#include "dependency_mining/dependency_validator.hpp"
#include "dependency_mining/pqp_analyzer.hpp"
#include "dependency_mining/util.hpp"
#include "utils/abstract_plugin.hpp"

/* Dependency Mining / Optimization doc
 *
 *  for enabling specific optimizations and dependency mining for them:
 *      - check switches at src/dependency_usage_config.hpp for default values
 *      - provide a JSON file with own values via CLI option --dep_config <usage_config_path>
 *      - an exemplary file can be found at <root_dir>/dependency_config.json
 *  when mining plugins/to turn off all optimizations:
 *      - set "preset_constraints" to false in the config file
 *      - otherwise,
 *          - some candidates might not be generated
 *          - UCC mining will notice that dependency is already set and do early-out
 *  to mine dependencies, load plugin
 *      - using CLI option --dep_mining_plugin <plugin_path>
 *      - <plugin_path> is the absolute path to the plugin library
 *      - i.e., <build_dir>/lib/libhyriseDependencyMiningPlugin.[so|dylib]
 *      - to restrict mining:
 *          - check switches at src/dependency_mining_config.hpp for default values
 *          - provide a JSON file with own values via CLI option --mining_config <mining_config_path>
 *          - an exemplary file can be found at <root_dir>/mining_config.json
 */
namespace opossum {

class DependencyMiningPlugin : public AbstractPlugin {
 public:
  DependencyMiningPlugin();
  std::string description() const final;

  void start() final;

  void stop() final;

 protected:
  std::shared_ptr<DependencyCandidateQueue> _queue;
  PQPAnalyzer _pqp_analyzer;
  std::vector<std::unique_ptr<DependencyValidator>> _dependency_validators;
};

}  // namespace opossum
