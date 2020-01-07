#include "anti_caching_plugin.hpp"

#include <iostream>

#include "hyrise.hpp"

namespace opossum {

const std::string AntiCachingPlugin::description() const
{
  return "AntiCaching Plugin";
}

void AntiCachingPlugin::start()
{
  std::cout << "AntiCaching Plugin starting up.\n";
  _evaluate_statistics_thread =
    std::make_unique<PausableLoopThread>(REFRESH_STATISTICS_INTERVAL, [&](size_t) { _evaluate_statistics(); });

}

void AntiCachingPlugin::stop()
{
  std::cout << "AntiCaching Plugin stopping.\n";

  _evaluate_statistics_thread.reset();
}

void AntiCachingPlugin::_evaluate_statistics()
{
  std::cout << "Evaluating statistics\n";
  // warum greifen wir die Statistiken nicht hier ab und setzen sie dann in den Segmenten zurück?
  // Dann können wir an dieser Stelle die Daten auswerten.
  // hash über Tabellennamen, spalte und segmentnr.
  // Ich finde diese Idee super.
  // Plugin kann gesamelte Daten abspeichern
  // Wir haben bestimmt die Möglichkeit auf das Plugin zuzugreifen
  // save to file...
  // data["table"]["column"]["segment"]
  // wir iterieren immer über den table manager
  // Beim Export, wird die meta_datei angelegt
  // Eine genaue Definition der einzlenen Ereignisse wäre echt super.
}

EXPORT_PLUGIN(AntiCachingPlugin)

} // namespace opossum