#pragma once

namespace opossum {

/**
 * This is an abstract class for all settings objects.
 * Settings register itself at the SettingsManager.
 * Settings provide a getter/setter with a string.
 * When the set(...) method ia called, a setting invokes a method of its parent that triggers
 * the immediate appliance of the changed value.
 * Settings have a unique name that consists of the name of its parent and the setting
 * name (e.g. "TaskScheduler.workers")
 */
class AbstractSetting : private Noncopyable {
 public:
  AbstractSetting() = default;

  virtual ~AbstractSetting() = default;

  virtual const std::string& name() const = 0;

  virtual const std:string& get() const;

  virtual void set(const std::string& value);
};

}  // namespace opossum
