"""Azure Login."""

# import: standard
import shlex
from typing import Iterable
from typing import Optional
from typing import Sequence

# import: internal
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import CommandResult
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import run_command

# import: external
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential


class AzCopyServicePrincipal:
    """Thin wrapper around AzCopy that authenticates with a Service Principal and runs
    AzCopy commands (e.g., copy) after logging in using Azure CLI.

    Example:
        client = AzCopyServicePrincipal(
            client_id=AZ_CLIENT_ID,
            client_secret=AZ_CLIENT_SECRET,
            tenant_id=AZ_TENANT_ID
        )
        client.login()
        client.copy("https://src", "https://dst", extra_args=["--recursive=true"])
        client.logout()
    """

    def __init__(self, client_id: str, client_secret: str, tenant_id: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id

    @retry(
        wait=wait_exponential(multiplier=1.5, min=10, max=120),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def login(self) -> CommandResult:
        """Perform Azure CLI login using a Service Principal.

        Uses:
            az login --service-principal \
                --username "${AZ_CLIENT_ID}" \
                --password "${AZ_CLIENT_SECRET}" \
                --tenant "${AZ_TENANT_ID}"
        """
        cmd = (
            "az login --service-principal "
            f"--username {shlex.quote(self.client_id)} "
            f"--password {shlex.quote(self.client_secret)} "
            f"--tenant {shlex.quote(self.tenant_id)}"
        )

        result = run_command(command=cmd)
        if result.exit_code != 0:
            raise ValueError(
                f"Azure CLI login failed.\nOutput: {result.output}\nError: {result.error}"
            )
        return result

    @retry(
        wait=wait_exponential(multiplier=1.5, min=10, max=120),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def copy(
        self,
        source_url: str,
        dest_url: str,
        extra_args: Optional[Iterable[str]] = None,
    ) -> CommandResult:
        """Run `azcopy copy` from source to destination.

        Args:
            source_url: Full source URL (e.g., blob/container URL; can include SAS if required).
            dest_url: Full destination URL.
            extra_args: Optional iterable of additional AzCopy flags (e.g., ["--recursive=true"]).

        Returns:
            CommandResult from run_command.
        """
        args: Sequence[str] = list(extra_args or [])
        # Build command safely; quote URLs/args
        quoted_args = " ".join(shlex.quote(a) for a in args)
        cmd = f"azcopy copy {shlex.quote(source_url)} {shlex.quote(dest_url)} {quoted_args}".strip()

        result = run_command(command=cmd)
        if result.exit_code != 0:
            raise ValueError(
                "AzCopy copy failed.\n"
                f"Command: {cmd}\n"
                f"Output: {result.output}\nError: {result.error}"
            )
        return result

    def logout(self) -> CommandResult:
        """Clear AzCopy credentials from the local AzCopy cache."""
        cmd = "azcopy logout"
        result = run_command(command=cmd)
        if result.exit_code != 0:
            raise ValueError(
                f"AzCopy logout failed.\nOutput: {result.output}\nError: {result.error}"
            )
        return result
