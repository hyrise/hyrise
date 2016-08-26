#!lua

-- Install pre-commit hook for linting
if os.execute("test -x .git/hooks/pre-commit") ~= 0 then
  os.execute("touch .git/hooks/pre-commit")
  os.execute("echo '#!/bin/sh\necho \"Linting all code, this may take a while...\"\n\nfind src -iname *.cpp -o -iname *.hpp | while read line;\ndo\n    if ! python cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright --linelength=120 $line >/dev/null 2>/dev/null\n    then\n        echo ERROR: Linting error occured. Execute \\\"premake4 lint\\\" for details\n        exit 1\n    fi\ndone\n\nif [ $? != 0 ]\nthen\n    exit 1\nfi\n\necho \"Success, no linting errors found!\"' >> .git/hooks/pre-commit")
  os.execute("chmod +x .git/hooks/pre-commit")
  os.execute("echo Successfully installed pre-commit hook.")
end

-- TODO try LTO/whole program

if not _OPTIONS["compiler"] then
   _OPTIONS["compiler"] = "gcc"
end

if _OPTIONS["compiler"] == "clang" then
  toolset = "clang"
else
  if os.execute("gcc-6 -v") == 0 then
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
      prebuildcommands { "find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"" }

   configuration "Release"
      defines { "NDEBUG" }
      flags { "OptimizeSpeed" }
      prebuildcommands { "find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"" }

   newoption {
      trigger     = "compiler",
      value       = "clang||gcc",
      description = "Choose a compiler",
      allowed = {
         { "gcc",    "gcc of version 6 or higher" },
         { "clang",  "clang llvm frontend" }
      }
   }

-- Registering linting and formatting as actions for premake is not optimal, make targets would be the preferable option, but impossible to generate or really hacky

newaction {
   trigger     = "lint",
   description = "Lint the code",
   execute = function ()
      os.execute("find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} python cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright --linelength=120 {}")
   end
}

newaction {
   trigger     = "format",
   description = "Format the code",
   execute = function ()
      os.execute("find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"")
   end
}
