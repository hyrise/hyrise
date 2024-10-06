#pragma once

#include <boost/process.hpp>

namespace hyrise {

const std::string PDGF_DIRECTORY_ROOT = "../../pdgf/original";

class PdgfProcess {
 public:
  explicit PdgfProcess(std::string pdgf_directory_root);

  void run();
  void wait();

 protected:
  std::string _pdgf_directory_root;
  boost::process::child _child;
  std::vector<std::string> _arguments;

  void _configure_numa();
  void _configure_jvm();
  void _configure_pdgf_properties();
  void _configure_pdgf_arguments();
};

}  // namespace hyrise
