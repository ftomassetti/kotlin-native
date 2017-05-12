#include <KotlinLanguageRuntime.h>
#include <lldb/Core/PluginManager.h>
#include <lldb/API/SBDebugger.h>

namespace lldb_private {
lldb_private::LanguageRuntime*
KotlinLanguageRuntime::CreateInstance(Process *process, lldb::LanguageType language) {
  if (language == eLanguageTypeKotlin)
    return new KotlinLanguageRuntime(process);
  return nullptr;
}

static ConstString g_name("konan");

void KotlinLanguageRuntime::Initialize() {
  PluginManager::RegisterPlugin(g_name, "Kotlin Language Runtime", CreateInstance);
}

void KotlinLanguageRuntime::Terminate() {
  PluginManager::UnregisterPlugin(CreateInstance);
}

lldb_private::ConstString KotlinLanguageRuntime::GetPluginName() {
  return g_name;
}

uint32_t KotlinLanguageRuntime::GetPluginVersion() { return 1;}
}

namespace lldb {
  bool PluginInitialize(SBDebugger) {
    lldb_private::KotlinLanguageRuntime::Initialize();
    return true;
  }
}

