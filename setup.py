# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

if sys.platform.startswith("win"):
    raise RuntimeError("Windows builds are not supported for pinpoint-python-agent.")

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext


class CMakeBuild(build_ext):
    def build_extension(self, ext: Extension) -> None:
        extdir = (Path.cwd() / self.get_ext_fullpath(ext.name)).parent.resolve()

        # Is a wheel-repair step going to run after this build? cibuildwheel
        # exports CIBUILDWHEEL, the manylinux images export AUDITWHEEL_PLAT.
        # It decides who is responsible for the transitive shared libraries
        # (gRPC, protobuf, abseil, yaml-cpp, fmt), which BUILD_SHARED_LIBS in
        # CMakeLists.txt makes separate .so/.dylib files that _native links
        # directly:
        #
        # - With a repair step, auditwheel/delocate vendor them into the wheel
        #   themselves, resolving them out of this build tree, so anything left
        #   beside _native is shipped a second time and loaded by neither. That
        #   duplication was 231 MB of the 325 MB a cp312 wheel unpacked to.
        # - Without one -- `pip install .`, or an sdist install on a platform
        #   that has no wheel -- nothing bundles anything, so they have to land
        #   beside _native for its $ORIGIN/@loader_path RUNPATH to find them at
        #   import time.
        #
        # macOS is deliberately excluded, so it keeps the second layout even
        # under cibuildwheel: delocate resolves a binary's @rpath entries only
        # through that binary's own LC_RPATHs, and _native carries just
        # @loader_path, so moving the dylibs away fails the repair outright
        # ("@rpath/libabsl_spinlock_wait.dylib not found, requested by
        # _native.cpython-311-darwin.so"). Linux's auditwheel keys by soname and
        # walks libpinpoint_cpp's RUNPATH into the build tree, which is why the
        # slim layout resolves there. Slimming macOS too means giving _native an
        # rpath into the dependency tree first.
        repaired = bool(os.environ.get("CIBUILDWHEEL") or os.environ.get("AUDITWHEEL_PLAT"))
        if sys.platform.startswith("darwin"):
            repaired = False

        cmake_args = [
            # The interpreter this build runs under is the one the extension is
            # for (FindPython's hint; CMakeLists uses find_package(Python3)).
            f"-DPython3_EXECUTABLE={sys.executable}",
            f"-DCMAKE_BUILD_TYPE={'Debug' if self.debug else 'Release'}",
        ]
        if not repaired:
            cmake_args.append(f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}{os.sep}")
        # CMAKE_ARGS comes after the defaults, so a user -D wins (last one
        # takes effect) — e.g. -DCMAKE_TOOLCHAIN_FILE for vcpkg.
        cmake_args += shlex.split(os.environ.get("CMAKE_ARGS", ""))
        # ninja is a build requirement (pyproject.toml), so it is on PATH.
        if "CMAKE_GENERATOR" not in os.environ:
            cmake_args += ["-GNinja"]

        # cibuildwheel sets ARCHFLAGS on macOS to pick the wheel's target arch.
        if sys.platform.startswith("darwin"):
            archs = re.findall(r"-arch (\S+)", os.environ.get("ARCHFLAGS", ""))
            if archs:
                cmake_args += ["-DCMAKE_OSX_ARCHITECTURES={}".format(";".join(archs))]

        build_args = [f"-j{self.parallel}"] if self.parallel else []

        build_temp = Path(self.build_temp) / ext.name
        build_temp.mkdir(parents=True, exist_ok=True)

        subprocess.run(
            ["cmake", os.fspath(Path.cwd()), *cmake_args], cwd=build_temp, check=True
        )
        subprocess.run(
            ["cmake", "--build", ".", *build_args], cwd=build_temp, check=True
        )

        if not repaired:
            # CMake wrote straight into the package directory; nothing to move.
            return

        # Take only the extension and the core it sits next to out of the build
        # tree, and leave the dependency tree where the repair step will find
        # it. CMakeLists' POST_BUILD step has already put libpinpoint_cpp's
        # SONAME file and the real file it points at beside _native here; the
        # repair step leaves those two alone because they are in the wheel.
        ext_path = Path(self.get_ext_fullpath(ext.name))
        ext_path.parent.mkdir(parents=True, exist_ok=True)

        built = sorted(build_temp.glob("_native*.so"))
        if not built:
            msg = f"cmake produced no _native*.so under {build_temp}"
            raise RuntimeError(msg)
        self.copy_file(os.fspath(built[0]), os.fspath(ext_path))

        cores = sorted(
            [
                *build_temp.glob("libpinpoint_cpp.so*"),
                *build_temp.glob("libpinpoint_cpp*.dylib"),
            ]
        )
        if not cores:
            msg = f"cmake copied no libpinpoint_cpp beside _native in {build_temp}"
            raise RuntimeError(msg)
        for core in cores:
            self.copy_file(os.fspath(core), os.fspath(ext_path.parent / core.name))


setup(
    ext_modules=[Extension("pinpoint._native", sources=[])],
    cmdclass={"build_ext": CMakeBuild},
)
