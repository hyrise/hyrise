#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>
#include <boost/process.hpp>
#include <thread>

#include "types.hpp"

namespace hyrise {

const std::string PDGF_DIRECTORY_ROOT = "../../pdgf/original";

class PdgfProcess : Noncopyable {
 public:
  static PdgfProcess for_schema_generation(std::string schema_config_file, std::string schema_generation_file, std::string pdgf_directory_root,
                                           uint32_t work_unit_size, uint32_t pdgf_num_cores, float scale_factor);
  static PdgfProcess for_data_generation(std::string schema_config_file, std::string schema_generation_file, std::string pdgf_directory_root,
                                         uint32_t work_unit_size, uint32_t pdgf_num_cores, float scale_factor);

  ~PdgfProcess();

  void run();
  void await_teardown();

  void set_column_filter(std::shared_ptr<std::set<std::string>>& columns_to_generate);

 protected:
  explicit PdgfProcess(std::string schema_config_file, std::string schema_generation_file, std::string pdgf_directory_root,
                       uint32_t work_unit_size, uint32_t pdgf_num_cores, std::string pdgf_command, float scale_factor);

  bool _has_run = false;
  bool _data_transmission_complete = false;

  std::string _schema_config_file;
  std::string _schema_generation_file;
  std::string _pdgf_directory_root;
  uint32_t _pdgf_work_unit_size;
  uint32_t _pdgf_num_cores;
  std::string _pdgf_command;
  float _scale_factor;
  std::vector<std::string> _arguments;
  std::shared_ptr<std::set<std::string>> _columns_to_generate;

  boost::process::child _child;
  boost::process::ipstream _child_out;
  boost::process::ipstream _child_err;
  std::vector<std::thread> _reader_threads;

  void _monitor_liveliness();

  void _configure_numa();
  void _configure_jvm();
  void _configure_pdgf_properties();
  void _configure_pdgf_arguments();
};

}  // namespace hyrise
