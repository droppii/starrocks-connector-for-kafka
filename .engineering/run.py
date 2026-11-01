#!/usr/bin/env python3
"""Repo-owned profile entry point; workspace and permissions belong to the agent platform.

Python 3.9+. Commands: list; check; run PROFILE --task WORK-ITEM.
The same source is copied into .engineering/run.py when updating repo profiles.
"""
import argparse
import atexit
import ast
from datetime import datetime, timezone
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
SOURCE_READER = None
SPARSE_PATHS = None


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])


def read_source(name):
    global SOURCE_READER, SPARSE_PATHS
    path = ROOT / name
    if path.is_symlink() or not path.resolve().is_relative_to(ROOT):
        raise ValueError(f"Path outside regular checkout files: {name}")
    if path.exists():
        return path.read_bytes()
    if SPARSE_PATHS is None:
        SPARSE_PATHS = {entry[2:] for entry in git("ls-files", "-t", "-z").decode().split("\0") if entry.startswith("S ")}
    if name not in SPARSE_PATHS:
        raise FileNotFoundError(f"Missing working-tree input: {name}")
    if "\n" in name or "\r" in name:
        raise ValueError("Unsupported newline in source path")
    if SOURCE_READER is None:
        SOURCE_READER = subprocess.Popen(["git", "-C", str(ROOT), "cat-file", "--batch"],
                                         stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    SOURCE_READER.stdin.write(("HEAD:" + name + "\n").encode())
    SOURCE_READER.stdin.flush()
    header = SOURCE_READER.stdout.readline().split()
    if len(header) != 3 or header[1] != b"blob":
        raise ValueError(f"Not a source blob at HEAD: {name}")
    data = SOURCE_READER.stdout.read(int(header[2]))
    SOURCE_READER.stdout.read(1)
    return data


def close_source_reader():
    if SOURCE_READER is not None:
        SOURCE_READER.stdin.close()
        SOURCE_READER.stdout.close()
        SOURCE_READER.wait()


atexit.register(close_source_reader)


def validate(config):
    if config.get("kind") != "repository-execution-profiles" or config.get("schemaVersion") != 1:
        raise ValueError("Unsupported profile format")
    ids = set()
    for profile in config["profiles"]:
        if profile["id"] in ids:
            raise ValueError("Duplicate profile ID")
        ids.add(profile["id"])
        for name in profile["requiredFiles"]:
            read_source(name)
        if profile["kind"] not in ("source-check", "command"):
            raise ValueError("Unsupported execution kind")
        if profile["kind"] == "command":
            if profile.get("evidenceKind") not in ("static-analysis", "build", "documentation"):
                raise ValueError("Behavior tests require a dedicated reporter-aware profile")
            if not profile.get("argv") or not all(isinstance(a, str) for a in profile["argv"]):
                raise ValueError("Command must be a nonempty argv array")
            cwd = ROOT / profile["cwd"]
            if not cwd.resolve().is_relative_to(ROOT):
                raise ValueError("Command cwd outside checkout")
    if not ids:
        raise ValueError("Empty profile catalog")


def interrupt(signum, frame):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["list", "check", "run"])
    parser.add_argument("profile", nargs="?")
    parser.add_argument("--task")
    args = parser.parse_args()
    config = json.loads((ROOT / ".engineering/profiles.json").read_text())
    validate(config)
    if args.action == "list":
        for p in config["profiles"]:
            print(p["id"] + " — " + p["purpose"] + " [" + p["evidenceKind"] + "]")
        return 0
    if args.action == "check":
        print(f'Profile contract and source references valid: {len(config["profiles"])}; no application tests executed.')
        return 0
    if not args.task:
        parser.error("run requires --task")
    selected = next((p for p in config["profiles"] if p["id"] == args.profile), None)
    if not selected:
        parser.error("choose a profile ID from list")
    out = ROOT / ".engineering/runs" / uuid.uuid4().hex
    out.mkdir(parents=True, mode=0o700)
    result = {"task": args.task, "profile": selected["id"], "evidenceKind": selected["evidenceKind"],
              "startedAt": datetime.now(timezone.utc).isoformat(), "status": "running",
              "revision": git("rev-parse", "HEAD").decode().strip(), "inputs": {},
              "profileSha256": hashlib.sha256((ROOT / ".engineering/profiles.json").read_bytes()).hexdigest(),
              "runnerSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "trackedDiffSha256": hashlib.sha256(git("diff", "HEAD", "--binary")).hexdigest(),
              "limitations": selected["limitations"], "runtimeIsolation": "provided by calling platform; not enforced by this script"}
    (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(f"Evidence: {out}", flush=True)
    signal.signal(signal.SIGTERM, interrupt)
    code = 1
    process = None
    try:
        if selected["kind"] == "source-check":
            tracked = git("ls-files", "--cached", "--others", "--exclude-standard", "-z").decode().split("\0")
            paths = sorted({f for f in tracked if f and any(fnmatch.fnmatchcase(f, g) for g in selected["patterns"])
                            and not any(fnmatch.fnmatchcase(f, g) for g in selected.get("exclude", []))})
            if not paths:
                raise ValueError("No source files selected; this is not a successful check")
            for name in paths:
                data = read_source(name)
                if name.endswith(".py"):
                    ast.parse(data, filename=name)
                elif name.endswith(".json"):
                    json.loads(data)
                elif name.endswith((".xml", ".csproj", ".props")):
                    ET.fromstring(data)
                result["inputs"][name] = hashlib.sha256(data).hexdigest()
            result["checkedFiles"] = len(paths)
            result["checks"] = "Python AST / strict JSON / XML where applicable; other selected files are fingerprinted only"
            code = 0
        else:
            for name in selected["requiredFiles"]:
                path = ROOT / name
                if not path.is_file():
                    raise ValueError(f"Materialize sparse-checkout input before execution: {name}")
                result["inputs"][name] = hashlib.sha256(read_source(name)).hexdigest()
            for path in selected.get("preparedPaths", []):
                if not (ROOT / path).exists():
                    raise ValueError(f"Prepare dependencies/toolchain first; missing {path}")
            result["argv"] = selected["argv"]
            result["cwd"] = selected["cwd"]
            # Only non-secret process settings are inherited; this is not a security sandbox.
            env = {k: os.environ[k] for k in ("PATH", "LANG", "LC_ALL", "JAVA_HOME", "DOTNET_ROOT") if k in os.environ}
            home = out / "home"
            home.mkdir()
            env.update({"HOME": str(home), "CI": "true", "TZ": "UTC"})
            with (out / "command.log").open("w") as log:
                process = subprocess.Popen(selected["argv"], cwd=ROOT / selected["cwd"], env=env,
                                           stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                code = process.wait(timeout=selected.get("timeoutSeconds", 300))
            result["exitCode"] = code
        result["status"] = "passed" if code == 0 else "failed"
    except subprocess.TimeoutExpired:
        result["status"] = "timed_out"
        code = 124
    except KeyboardInterrupt:
        result["status"] = "interrupted"
        code = 130
    except (OSError, ValueError, SyntaxError, ET.ParseError, subprocess.SubprocessError) as error:
        result["status"] = "failed"
        result["error"] = str(error)
    finally:
        if process is not None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        result["finishedAt"] = datetime.now(timezone.utc).isoformat()
        (out / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        print(result["status"] + " (" + selected["evidenceKind"] + ")")
    return code


if __name__ == "__main__":
    sys.exit(main())
