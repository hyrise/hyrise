#!lua

if os.execute("gcc-6 -v") == 0 then
    premake.gcc.cc  = 'gcc-6'
    premake.gcc.cxx = 'g++-6'
end

solution "Opossum"
   configurations { "Debug", "Release" }
   platforms "x64"
   flags { "FatalWarnings", "ExtraWarnings" }

project "Opossum"
   kind "ConsoleApp"
   language "C++"
   targetdir "build/"

   buildoptions "-std=c++1z"

   files { "**.hpp", "**.cpp" }
   includedirs { "src/lib/" }

   configuration "Debug"
      defines { "DEBUG" }
      flags { "Symbols" }

   configuration "Release"
      defines { "NDEBUG" }
      flags { "OptimizeSpeed" }