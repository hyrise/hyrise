#!lua

-- clang || gcc
-- compiler = "clang"

if not _OPTIONS["compiler"] then
   _OPTIONS["compiler"] = "gcc"
end

if _OPTIONS["compiler"] == "clang" then
  print("clang")
  toolset = "clang"
else
  if os.execute("gcc-6 -v") == 0 then
    print("gcc")
    premake.gcc.cc  = 'gcc-6'
    premake.gcc.cxx = 'g++-6'
  else
    error("gcc version 6 required. Aborting.")
  end
end

solution "Opossum"
   configurations { "Debug", "Release" }
   platforms "x64"
   flags { "FatalWarnings", "ExtraWarnings" }

project "Opossum"
   kind "ConsoleApp"
   language "C++"
   targetdir "build/"

   buildoptions { "-std=c++1z" }

   files { "**.hpp", "**.cpp" }
   includedirs { "src/lib/", "/usr/local/include" }

   configuration "Debug"
      defines { "DEBUG" }
      flags { "Symbols" }

   configuration "Release"
      defines { "NDEBUG" }
      flags { "OptimizeSpeed" }

   newoption {
      trigger     = "compiler",
      value       = "clang||gcc",
      description = "Choose a compiler",
      allowed = {
         { "gcc",    "gcc of version 6 or higher" },
         { "clang",  "clang llvm frontend" }
      }
   }
