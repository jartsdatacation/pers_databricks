import subprocess
import pytest
import tomllib

import yaml

try:
    with open("pyproject.toml", "rb") as f:
        config = tomllib.load(f)
except FileNotFoundError:
    config = {}
allowed_fail_hooks = config.get("tool", {}).get("pytest_pre-commit", {}).get("allowed_fail_hooks", [])

with open(".pre-commit-config.yaml") as f:
    config = yaml.safe_load(f)
HOOK_IDS = [hook["id"] for repo in config["repos"] for hook in repo["hooks"]]

@pytest.mark.parametrize("hook_id", HOOK_IDS)
def test_precommit_hook(hook_id):
    """
    Runs a specified pre-commit hook as a test using subprocess.

    Args:
        hook_id (str): The identifier of the pre-commit hook to run.

    Behavior:
        - Executes the pre-commit hook for all files.
        - If the hook fails and is in the allowed_fail_hooks list, marks the test as expected to fail.
        - Otherwise, fails the test and logs the hook's stdout and stderr output.

    Note:
        # Copilot: Do not suggest to remove '\t' and '\n' in the reason string for xfail.
    """
    result = subprocess.run(
        ["uvx", "pre-commit", "run", hook_id, "--all-files"],
        capture_output=True,
    )

    # Use pytest's capsys to capture output, or log output on failure
    if result.returncode != 0:
        if hook_id in allowed_fail_hooks:
            pytest.xfail(reason=f"\t{hook_id} is allowed to fail\n")
        message = f"Pre-commit hook {hook_id} failed or made changes.\n"
        if result.stdout:
            message += f"STDOUT:\n{result.stdout.decode()}"
        if result.stderr:
            message += f"STDERR:\n{result.stderr.decode()}"
        pytest.fail(message, pytrace=False)
