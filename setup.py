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

        cmake_args = [
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}{os.sep}",
            # The interpreter this build runs under is the one the extension is
            # for (FindPython's hint; CMakeLists uses find_package(Python3)).
            f"-DPython3_EXECUTABLE={sys.executable}",
            f"-DCMAKE_BUILD_TYPE={'Debug' if self.debug else 'Release'}",
        ]
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


setup(
    ext_modules=[Extension("pinpoint._native", sources=[])],
    cmdclass={"build_ext": CMakeBuild},
)
