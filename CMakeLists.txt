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

# Tell _native to look for its sibling dylib in the same directory at runtime.
# @loader_path is the dir containing _native.so; $ORIGIN is the Linux analog.
set_target_properties(_native PROPERTIES
        BUILD_WITH_INSTALL_RPATH TRUE
        INSTALL_RPATH "$<IF:$<BOOL:${APPLE}>,@loader_path,$ORIGIN>")

# Let libpinpoint_cpp look beside itself too. Without this it reaches gRPC,
# protobuf and abseil only through the absolute dependency-tree paths CMake
# bakes in, which do not survive a `pip install .` or an sdist install: the
# build directory is gone by the time the extension is imported, and a linker
# defaulting to --as-needed (most distributions) drops _native's own direct
# links to those libraries, so its $ORIGIN does not cover them either. Appending
# to the build RPATH rather than replacing it keeps the dependency-tree entries
# that a wheel repair step (auditwheel/delocate) resolves through.
set_target_properties(pinpoint_cpp PROPERTIES
        BUILD_RPATH "$<IF:$<BOOL:${APPLE}>,@loader_path,$ORIGIN>")

