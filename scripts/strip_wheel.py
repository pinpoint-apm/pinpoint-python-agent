#!/usr/bin/env python3
"""Strip the ELF shared objects inside a repaired Linux wheel, in place.

Run by pyproject.toml's repair-wheel-command after `auditwheel repair`, for two
reasons that each roughly double the wheel on their own:

- BoringSSL's CMake adds -ggdb unconditionally, so the libcrypto/libssl that
  gRPC's FetchContent builds carry their full DWARF (24 MB of the two). macOS
  never sees this: ld64 leaves DWARF in the .o files, so the dylibs stay small.
- auditwheel drives patchelf once per change (soname, NEEDED, RPATH), and each
  run that grows .dynstr moves it into a fresh page-aligned PT_LOAD at the end
  of the file without reclaiming the old copy. libgrpc alone ends up with 6.8 MB
  of bytes no section refers to. strip rewrites the section layout and drops
  that dead space along with the debug info.

Stripping after the repair, not before, is what handles the second point; strip
keeps .dynsym, so the libraries still resolve against each other.

    scripts/strip_wheel.py WHEEL...
"""

import base64
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile


def is_elf(path: str) -> bool:
    with open(path, "rb") as f:
        return f.read(4) == b"\x7fELF"


def record_line(root: str, name: str) -> str:
    path = os.path.join(root, name)
    with open(path, "rb") as f:
        digest = hashlib.sha256(f.read()).digest()
    b64 = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return f"{name},sha256={b64},{os.path.getsize(path)}"


def strip_wheel(wheel: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(wheel) as zf:
            names = zf.namelist()
            zf.extractall(tmp)

        (record,) = [n for n in names if n.endswith(".dist-info/RECORD")]
        stripped = []
        for name in names:
            path = os.path.join(tmp, name)
            if os.path.isfile(path) and ".so" in os.path.basename(name) and is_elf(path):
                before = os.path.getsize(path)
                subprocess.run(["strip", "--strip-unneeded", path], check=True)
                stripped.append((name, before, os.path.getsize(path)))

        # RECORD lists every file with its hash and size, and pip verifies it
        # at install time, so the stripped entries have to be rewritten.
        with open(os.path.join(tmp, record)) as f:
            lines = f.read().splitlines()
        changed = {name for name, _, _ in stripped}
        lines = [
            record_line(tmp, line.split(",")[0]) if line.split(",")[0] in changed else line
            for line in lines
        ]
        with open(os.path.join(tmp, record), "w") as f:
            f.write("\n".join(lines) + "\n")

        # Same member order as the original, RECORD still last.
        out = os.path.join(tmp, "out.whl")
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in names:
                zf.write(os.path.join(tmp, name), name)
        shutil.move(out, wheel)

    for name, before, after in stripped:
        print(f"stripped {name}: {before} -> {after} bytes")
    print(f"{wheel}: {os.path.getsize(wheel)} bytes")


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    for wheel in argv:
        strip_wheel(wheel)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
