"""
Built-in FaaSr functions.
These are framework-provided functions automatically available.
"""

from .vm_start import vm_start
from .vm_stop import vm_stop
from .vm_poll import vm_poll

__all__ = [
    'vm_start',
    'vm_stop',
    'vm_poll'
]