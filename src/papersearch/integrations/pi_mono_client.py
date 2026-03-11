from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from typing import Optional


@dataclass
class PiRunResult:
    ok: bool
    command: list[str]
    returncode: int
    stdout: str
    stderr: str


class PiMonoClient:
    def __init__(self, pi_command: str = "pi", timeout_seconds: int = 180):
        self.pi_command = pi_command
        self.timeout_seconds = timeout_seconds

    def is_available(self) -> bool:
        return shutil.which(self.pi_command) is not None

    def list_models(self, provider: str | None = None, search: str | None = None) -> PiRunResult:
        args = [self.pi_command]
        if provider:
            args += ["--provider", provider]
        args += ["--list-models"]
        if search:
            args.append(search)
        return self._run(args)

    def prompt(
        self,
        prompt: str,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        thinking: Optional[str] = None,
        cwd: Optional[str] = None,
    ) -> PiRunResult:
        if not (prompt or "").strip():
            raise ValueError("prompt is required")

        args = [self.pi_command, "-p", "--no-session"]
        if provider:
            args += ["--provider", provider]
        if model:
            args += ["--model", model]
        if thinking:
            args += ["--thinking", thinking]
        args.append(prompt)
        return self._run(args, cwd=cwd)

    def _run(self, command: list[str], cwd: Optional[str] = None) -> PiRunResult:
        try:
            p = subprocess.run(command, capture_output=True, text=True, timeout=self.timeout_seconds, cwd=cwd)
            return PiRunResult(
                ok=p.returncode == 0,
                command=command,
                returncode=p.returncode,
                stdout=(p.stdout or "").strip(),
                stderr=(p.stderr or "").strip(),
            )
        except FileNotFoundError:
            return PiRunResult(ok=False, command=command, returncode=127, stdout="", stderr="pi command not found")
        except subprocess.TimeoutExpired:
            return PiRunResult(ok=False, command=command, returncode=124, stdout="", stderr="pi command timeout")
