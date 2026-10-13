# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Run file-driven E2E cases against a built or installed tsfile-cli."""

import argparse
import difflib
import json
import hashlib
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile

HAS_POSIX = os.name != "nt"


def local_path(root, name):
    """Resolve a suite/workspace path without allowing absolute paths or '..'."""
    path = Path(name)
    if not name or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Expected a relative path within {root}: {name!r}")
    result = root / path
    # Check parents, while allowing the leaf itself to be a symlink fixture.
    if not result.parent.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"Path escapes {root}: {name!r}")
    return result


def read_expected(root, name):
    path = local_path(root, name)
    if path.is_symlink():
        raise ValueError(f"Expected file must not be a symlink: {path}")
    return path.read_bytes()


def remove_path(path):
    if path.is_symlink() or (path.exists() and not path.is_dir()):
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def prepare(root, work, actions):
    for action in actions:
        target = local_path(work, action["path"])
        remove_path(target)
        kind = action["type"]
        if kind == "absent":
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if kind == "file":
            target.write_bytes(read_expected(root, action["source"]))
        elif kind == "directory":
            target.mkdir(exist_ok=True)
        elif kind == "symlink":
            local_path(work, str(Path(action["path"]).parent / action["target"]))
            target.symlink_to(action["target"])
        elif kind == "fifo":
            os.mkfifo(target)
        else:
            raise ValueError(f"Unknown preparation type: {kind}")


def tree_state(work):
    result = {}
    for directory, dirs, files in os.walk(work, followlinks=False):
        for name in dirs + files:
            path = Path(directory) / name
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                kind = "symlink"
            elif stat.S_ISDIR(mode):
                kind = "directory"
            elif stat.S_ISFIFO(mode):
                kind = "fifo"
            elif stat.S_ISREG(mode):
                kind = "file"
            else:
                kind = "special"
            result[path.relative_to(work).as_posix()] = kind
    return result


def compare(expected, actual, label, differences):
    if expected == actual:
        return
    differences.append(f"{label}: output differs\n")
    expected_lines = expected.decode("utf-8", errors="backslashreplace").splitlines(
        True
    )
    actual_lines = actual.decode("utf-8", errors="backslashreplace").splitlines(True)
    differences.extend(
        difflib.unified_diff(expected_lines, actual_lines, "expected", "actual")
    )
    if expected_lines == actual_lines:
        differences.append(f"expected bytes: {expected!r}\nactual bytes: {actual!r}\n")


def version_output(data, expected_version, require_concrete_commit=False):
    match = re.fullmatch(
        rb"tsfile-cli (\S+) tsfile=(\S+) commit=([0-9a-f]{40}|unknown) "
        rb"built=(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\n",
        data,
    )
    if not match or match[1] != match[2]:
        raise ValueError(
            "CLI version output has missing or inconsistent build metadata"
        )
    if require_concrete_commit and match[3] == b"unknown":
        raise ValueError("CLI commit metadata must be concrete for this case")
    if expected_version and match[1].decode() != expected_version:
        raise ValueError(
            f"Expected CLI version {expected_version}, got {match[1].decode()}"
        )
    return b"tsfile-cli <VERSION> tsfile=<VERSION> commit=<COMMIT> built=<BUILT>\n"


def run_case(case, root, output, executable, timeout, expected_version):
    destination = local_path(output, case["id"])
    destination.mkdir(parents=True)
    work = destination / "work"
    work.mkdir()
    results = []
    for index, step in enumerate(case["steps"], 1):
        prefix = destination / f"{index:03d}"
        differences = []
        record = {"args": step["args"], "expected_exit_code": step["exit_code"]}
        try:
            actions = step.get("prepare", [])
            skip_reason = step.get("posix_only") if not HAS_POSIX else None
            if skip_reason:
                # Preserve ordinary fixture setup for later supported commands.
                # Unsupported special files are never created on Windows.
                actions = [a for a in actions if a["type"] not in ("fifo", "symlink")]
            prepare(root, work, actions)
            if skip_reason:
                results.append(
                    {"args": step["args"], "passed": True, "skipped": skip_reason}
                )
                continue
            env = dict(os.environ, LC_ALL="C", LANG="C", TZ="UTC")
            expected_files = {}
            for name, kind in tree_state(work).items():
                info = {"type": kind}
                target = local_path(work, name)
                if kind == "file":
                    info["contents"] = target.read_bytes()
                elif kind == "symlink":
                    info["target"] = os.readlink(target)
                expected_files[name] = info
            for name, info in step.get("files", {}).items():
                local_path(work, name)
                if info["type"] == "absent":
                    for existing in list(expected_files):
                        if existing == name or existing.startswith(name + "/"):
                            del expected_files[existing]
                else:
                    expected_files[name] = info
            stdin_data = read_expected(root, step["stdin"]) if "stdin" in step else b""
            with prefix.with_suffix(".stdout").open("wb") as out:
                with prefix.with_suffix(".stderr").open("wb") as err:
                    completed = subprocess.run(
                        [str(executable), *step["args"]],
                        cwd=work,
                        env=env,
                        input=stdin_data,
                        stdout=out,
                        stderr=err,
                        timeout=timeout,
                        check=False,
                    )
            record["actual_exit_code"] = completed.returncode
            prefix.with_suffix(".exit_code").write_text(f"{completed.returncode}\n")
            if completed.returncode != step["exit_code"]:
                differences.append(
                    f"exit code: expected {step['exit_code']}, actual {completed.returncode}\n"
                )
            stdout = prefix.with_suffix(".stdout").read_bytes()
            stderr = prefix.with_suffix(".stderr").read_bytes()
            if step.get("stdout_format") == "version":
                stdout = version_output(
                    stdout, expected_version, step.get("require_concrete_commit", False)
                )
            compare(read_expected(root, step["stdout"]), stdout, "stdout", differences)
            expected_stderr = read_expected(root, step["stderr"])
            # tools_main switches redirected stdout to binary mode on Windows,
            # while stderr keeps the CRT's native text-mode newlines.
            if os.name == "nt":
                expected_stderr = expected_stderr.replace(b"\n", b"\r\n")
            compare(expected_stderr, stderr, "stderr", differences)
            actual_files = tree_state(work)
            expected_kinds = {
                name: info["type"] for name, info in expected_files.items()
            }
            if expected_kinds != actual_files:
                differences.append(
                    f"filesystem: expected {expected_kinds!r}, actual {actual_files!r}\n"
                )
            for name, info in expected_files.items():
                target = local_path(work, name)
                if actual_files.get(name) != info["type"]:
                    continue
                if "source" in info:
                    compare(
                        read_expected(root, info["source"]),
                        target.read_bytes(),
                        name,
                        differences,
                    )
                elif "contents" in info:
                    compare(info["contents"], target.read_bytes(), name, differences)
                elif (
                    info["type"] == "symlink" and os.readlink(target) != info["target"]
                ):
                    differences.append(f"{name}: symlink target differs\n")
        except (OSError, ValueError, subprocess.TimeoutExpired) as error:
            differences.append(f"execution failed: {error}\n")
        record["passed"] = not differences
        if differences:
            prefix.with_suffix(".diff").write_text(
                "".join(differences), encoding="utf-8"
            )
        results.append(record)
        # Later steps may consume this command's output. Preserve this failure
        # without running dependent steps against incomplete files.
        if differences:
            break
    result = {
        "id": case["id"],
        "passed": all(s["passed"] for s in results),
        "steps": results,
    }
    if all("skipped" in step for step in results):
        result["skipped"] = "All steps require POSIX facilities"
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cli", required=True, help="Built or installed tsfile-cli executable"
    )
    parser.add_argument(
        "--suite", type=Path, default=Path(__file__).with_name("cases.json")
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Parent directory for retained per-run outputs and reports",
    )
    parser.add_argument(
        "--timeout", type=float, default=30, help="Timeout in seconds per command"
    )
    parser.add_argument(
        "--expected-version", help="Also require this version in --version cases"
    )
    parser.add_argument(
        "--case", action="append", help="Run only the named case; may be repeated"
    )
    args = parser.parse_args(argv)
    try:
        executable = shutil.which(args.cli)
        if not executable or args.timeout <= 0:
            raise ValueError(
                "--cli must identify an executable and --timeout must be positive"
            )
        executable = Path(executable).resolve()
        suite = json.loads(args.suite.read_text(encoding="utf-8"))
        if suite["schema_version"] != 1 or not suite["cases"]:
            raise ValueError("Expected a nonempty schema_version=1 suite")
        root = args.suite.resolve().parent
        cases = suite["cases"]
        ids = [case["id"] for case in cases]
        if len(set(ids)) != len(ids):
            raise ValueError("Case IDs must be unique")
        if args.case:
            unknown = set(args.case) - set(ids)
            if unknown:
                raise ValueError(f"Unknown cases: {sorted(unknown)}")
            cases = [case for case in cases if case["id"] in args.case]
        for case in cases:
            local_path(args.output_dir.resolve(), case["id"])
            if not case["steps"]:
                raise ValueError(f"Empty case: {case['id']}")
            for step in case["steps"]:
                if type(step["exit_code"]) is not int or not isinstance(
                    step["args"], list
                ):
                    raise ValueError(f"Invalid exit_code/args: {case['id']}")
                for name in (step["stdout"], step["stderr"]):
                    read_expected(root, name)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        args.output_dir = Path(tempfile.mkdtemp(prefix="run-", dir=args.output_dir))
        results = []
        for case in cases:
            if os.name == "nt" and case.get("posix_only"):
                results.append({"id": case["id"], "skipped": case["posix_only"]})
                print(f"SKIP {case['id']}: {case['posix_only']}")
                continue
            result = run_case(
                case,
                root,
                args.output_dir,
                executable,
                args.timeout,
                args.expected_version,
            )
            results.append(result)
            print(f"{'PASS' if result['passed'] else 'FAIL'} {case['id']}")
        failed = sum(not r.get("passed", True) for r in results)
        skipped = sum("skipped" in r for r in results)
        command_count = sum(len(c["steps"]) for c in cases)
        executed_count = sum(
            "actual_exit_code" in step
            for result in results
            for step in result.get("steps", [])
        )
        skipped_commands = sum(
            "skipped" in step for result in results for step in result.get("steps", [])
        )
        asset_hash = hashlib.sha256()
        for folder in ("fixtures", "golden"):
            for path in sorted((root / folder).rglob("*")):
                if path.is_file():
                    asset_hash.update(
                        path.relative_to(root).as_posix().encode() + b"\0"
                    )
                    asset_hash.update(hashlib.sha256(path.read_bytes()).digest())
        report = {
            "cli_sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
            "suite_sha256": hashlib.sha256(args.suite.read_bytes()).hexdigest(),
            "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "assets_sha256": asset_hash.hexdigest(),
            "commands": command_count,
            "executed_commands": executed_count,
            "skipped_commands": skipped_commands,
            "cli": str(executable),
            "suite": str(args.suite.resolve()),
            "failed": failed,
            "skipped": skipped,
            "cases": results,
        }
        (args.output_dir / "report.json").write_text(
            json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )
        print(
            f"{len(results) - failed - skipped} passed, {failed} failed, {skipped} skipped; results: {args.output_dir}"
        )
        return 1 if failed else 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Invalid E2E invocation or suite: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
