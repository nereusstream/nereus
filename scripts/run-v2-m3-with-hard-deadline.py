#!/usr/bin/env python3
"""Run one command in an isolated process group under an absolute hard deadline."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


TIMEOUT_EXIT_CODE = 124


class ForwardedSignal(Exception):
    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


def positive_seconds(value: str) -> float:
    seconds = float(value)
    if seconds <= 0:
        raise argparse.ArgumentTypeError("seconds must be positive")
    return seconds


def signal_process_group(process: subprocess.Popen[bytes], signum: int) -> None:
    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError:
        pass


def shell_exit_code(returncode: int) -> int:
    return returncode if returncode >= 0 else 128 - returncode


def stop_process_group(
    process: subprocess.Popen[bytes], signum: int, grace_seconds: float
) -> None:
    signal_process_group(process, signum)
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        signal_process_group(process, signal.SIGKILL)
        process.wait()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hard-deadline-seconds", required=True, type=positive_seconds)
    parser.add_argument("--termination-grace-seconds", required=True, type=positive_seconds)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    command = arguments.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        parser.error("a command is required after --")
    if arguments.termination_grace_seconds >= arguments.hard_deadline_seconds:
        parser.error("termination grace must be shorter than the hard deadline")

    process = subprocess.Popen(command, start_new_session=True)

    def forward(signum: int, _frame: object) -> None:
        raise ForwardedSignal(signum)

    signal.signal(signal.SIGINT, forward)
    signal.signal(signal.SIGTERM, forward)

    started = time.monotonic()
    soft_wait = arguments.hard_deadline_seconds - arguments.termination_grace_seconds
    try:
        try:
            return shell_exit_code(process.wait(timeout=soft_wait))
        except subprocess.TimeoutExpired:
            print(
                "hard-deadline supervisor: terminating process group before "
                f"the {arguments.hard_deadline_seconds:g}-second cap: {Path(command[0]).name}",
                file=sys.stderr,
                flush=True,
            )
            signal_process_group(process, signal.SIGTERM)
            remaining = max(0.0, arguments.hard_deadline_seconds - (time.monotonic() - started))
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                signal_process_group(process, signal.SIGKILL)
                process.wait()
            return TIMEOUT_EXIT_CODE
    except ForwardedSignal as interruption:
        stop_process_group(
            process,
            interruption.signum,
            arguments.termination_grace_seconds,
        )
        return 128 + interruption.signum


if __name__ == "__main__":
    raise SystemExit(main())
