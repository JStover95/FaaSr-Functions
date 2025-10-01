"""
VM module for FaaSr_py package - handles VM lifecycle for GitHub Actions workflows.
"""

from .detection import workflow_needs_vm, action_requires_vm
from .orchestration import orchestrate_vm, get_vm_strategy

__all__ = [
    "workflow_needs_vm",
    "action_requires_vm",
    "orchestrate_vm",
    "get_vm_strategy"
]