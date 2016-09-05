#!lua

-- Install pre-commit hook for linting if not installed yet or outdated
-- TODO: This should be part of the setup script, not the (pre)make file
if os.execute("test -x .git/hooks/pre-commit") ~= 0 or os.execute("md5 -q .git/hooks/pre-commit | grep cb1e4086756ddacb54ec87f775abd82b >/dev/null 2>/dev/null") ~= 0 then
  os.execute("touch .git/hooks/pre-commit")
  os.execute("echo '#!/bin/sh\necho \"Linting all code, this may take a while...\"\n\nfind src -iname *.cpp -o -iname *.hpp | while read line;\ndo\n    if ! python cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright --linelength=120 $line >/dev/null 2>/dev/null\n    then\n        echo \"ERROR: Linting error occured. Execute \\\"premake4 lint\\\" for details!\n(What kind of ***** would consider a commit without linting?)\"\n        exit 1\n    fi\ndone\n\nif [ $? != 0 ]\nthen\n    exit 1\nfi\n\necho \"Success, no linting errors found!\"\n\necho \"Testing the Opossum, grrrrr...\"\nmake -j test >/dev/null 2>/dev/null\nif ! ./build/test >/dev/null 2>/dev/null\nthen\n    echo \"ERROR: Testing error occured. Execute \\\"make test\\\" for details!\n(What kind of ***** would consider a commit without testing?)\"\n    exit 1\nfi\n\necho \"Success, no testing errors found!\"' > .git/hooks/pre-commit")
  os.execute("chmod +x .git/hooks/pre-commit")
  os.execute("echo Successfully installed pre-commit hook.")
end

-- TODO try LTO/whole program

function default(osName, actionName)
   if osName ~= nil and os.is(osName) == false then
      return
   end

   if _ACTION == nil then
      _ACTION = actionName
   end
end
 
default("linux", "gmake")
default("macosx", "gmake")

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

solution "opossum"
   configurations { "Debug", "Release" }
   platforms "x64"
   flags { "FatalWarnings", "ExtraWarnings" }
   language "C++"
   targetdir "build"
   buildoptions { "-std=c++1z -pthread" }
   includedirs { "src/lib/", "/usr/local/include" }

   configuration "Debug"
      defines { "IS_DEBUG=1" }
      flags { "Symbols" }
      prebuildcommands { "find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"" }
        -- TODO Shouldn't this be part of the pre-commit hook? "make" should never touch the code

   configuration "Release"
      defines { "IS_DEBUG=0" }
      flags { "OptimizeSpeed" }
      prebuildcommands { "find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"" }

project "googletest"
   kind "StaticLib"
   files { "third_party/googletest/googletest/src/gtest-all.cc" }
   includedirs { "third_party/googletest/googletest", "third_party/googletest/googletest/include" }

project "opossum"
   kind "StaticLib"
   files { "src/lib/**.hpp", "src/lib/**.cpp", "src/bin/server.cpp" }

project "playground"
   kind "ConsoleApp"
   links { "opossum" }
   files { "src/bin/playground.cpp" }

project "test"
   kind "ConsoleApp"

   defines { "IS_DEBUG=1" }

   links { "opossum", "googletest" }
   files { "src/test/**.hpp", "src/test/**.cpp" }
   includedirs { "third_party/googletest/googletest/include" }
   postbuildcommands { "./build/test" }

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

newaction {
   trigger     = "test",
   description = "Test the code",
   execute = function ()
      os.execute("make TestOpossum -j && ./build/TestOpossum")
   end
}
