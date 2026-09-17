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
# tree below -- ships side by side inside the wheel, so they all get their own
# directory as the FIRST rpath entry. @loader_path is the dir containing the
# loading binary; $ORIGIN is the Linux analog.
#
# It has to be the global default rather than a property on one target. A
# dependency left with only CMake's computed build rpath reaches its siblings
# through absolute paths into the build tree, and those break twice over:
#
# - The build directory is gone once the wheel is installed -- for a `pip
#   install .` or an sdist install, at a temporary path pip has deleted -- and
#   a linker defaulting to --as-needed drops _native's direct links, so its own
#   entry does not cover the gap either.
# - On macOS the wheel repair step reads that absolute path, decides the
#   dependency lives outside the wheel, and copies a second copy into
#   pinpoint/.dylibs. Two copies of libgpr at two paths means dyld loads both,
#   and absl aborts the import on the duplicate registration of the
#   grpc_experiments flag. (Linux is spared that one: glibc dedupes on SONAME.)
#
# This ADDS to the computed rpath rather than replacing it, which is why it is
# not BUILD_WITH_INSTALL_RPATH: the build-time tools, protoc above all, live in
# their own build directories and still need the computed entry to find the
# dylibs over in CMAKE_LIBRARY_OUTPUT_DIRECTORY.
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


