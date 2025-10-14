from .detection import workflow_needs_vm, action_requires_vm
from .github_runner import check_runner_online

__all__ = [
    'workflow_needs_vm',
    'action_requires_vm',
    'check_runner_online'
]