cmake_minimum_required(VERSION 3.21)

set(PINPOINT_CPP_AGENT_SOURCE_DIR
        "${CMAKE_CURRENT_SOURCE_DIR}/third_party/pinpoint-cpp-agent"
        CACHE PATH "Path to the pinpoint-cpp-agent git submodule")

project(pinpoint_python_agent LANGUAGES CXX)

if(WIN32)
    message(FATAL_ERROR "Windows builds are not supported for pinpoint-python-agent.")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)

# Every target here -- _native, pinpoint_cpp and the whole gRPC/protobuf/absl
# tree below -- gets its own directory as the FIRST rpath entry, ahead of the
# absolute build-tree paths CMake computes. @loader_path is the dir containing
# the loading binary; $ORIGIN is the Linux analog.
#
# It has to be the global default rather than a property on one target. Where
# setup.py ships the whole tree beside _native -- a `pip install .` or an sdist
# install, anywhere no wheel repair step follows -- a dependency reaching its
# siblings only through those computed paths finds nothing at import time: the
# build directory is gone, at a temporary path pip has already deleted, and a
# linker defaulting to --as-needed drops _native's direct links, so its own
# entry does not cover the gap either. On macOS that same layout also made
# delocate read the absolute path, decide the dependency lived outside the
# wheel, and copy a second one into pinpoint/.dylibs -- two copies of libgpr at
# two paths, so dyld loaded both and absl aborted the import on the duplicate
# registration of the grpc_experiments flag.
#
# The computed entries stay, rather than being replaced: they are how a repair
# step resolves the dependency tree out of the build directory, and how the
# build-time tools -- protoc above all, off in its own build directory -- find
# the libraries they link against.
set(CMAKE_BUILD_RPATH "$<IF:$<BOOL:${APPLE}>,@loader_path,$ORIGIN>")

include(FetchContent)
set(BUILD_SHARED_LIBS ON)
set(BUILD_STATIC_LIBS OFF)
set(BUILD_TESTING OFF)
set(BUILD_EXAMPLES OFF)

FetchContent_Declare(
        pybind11
        GIT_REPOSITORY "https://github.com/pybind/pybind11.git"
        GIT_TAG "v2.13.6"
        GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(pybind11)

# Missing submodule? add_subdirectory reports the path; see docs/development.md §1.
add_subdirectory(
        "${PINPOINT_CPP_AGENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_BINARY_DIR}/pinpoint-cpp-agent")

pybind11_add_module(_native src/_native.cpp)

# The submodule's `pinpoint_cpp` target carries its include dir as INTERFACE.
target_link_libraries(_native PRIVATE pinpoint_cpp)

# Copy libpinpoint_cpp next to _native after build: _native needs the SONAME
# file it links against (libpinpoint_cpp.2.dylib / .so.2) plus the real file
# that symlink points at.
#
# This does NOT make the package self-contained. BUILD_SHARED_LIBS above also
# makes gRPC, protobuf and abseil separate shared objects that _native links
# directly (`otool -L`/`ldd` shows the @rpath/$ORIGIN entries), and those stay
# in the dependency build tree. Wheels are only loadable because the repair step
# bundles them -- auditwheel on Linux, delocate on macOS (see
# [tool.cibuildwheel] in pyproject.toml). A plain in-tree build needs
# LD_LIBRARY_PATH/DYLD_LIBRARY_PATH pointed at them; scripts/dev.sh does
# that, and docs/development.md §5 explains why.
add_custom_command(TARGET _native POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:pinpoint_cpp>
                $<TARGET_FILE_DIR:_native>
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_SONAME_FILE:pinpoint_cpp>
                $<TARGET_FILE_DIR:_native>)


