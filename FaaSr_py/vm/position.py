"""
Utilities for determining function position in workflow DAG.
"""

import logging

logger = logging.getLogger("FaaSr_py.vm")

def get_action_position(faasr_payload, action_name):
    """
    Determine if an action is first, last, or middle in the workflow.
    
    Args:
        faasr_payload: The FaaSr workflow configuration payload
        action_name: Name of the action to check
        
    Returns:
        dict: Position information with is_first, is_last, and type fields
    """
    action_list = faasr_payload.get("ActionList", {})
    
    # Find all actions that invoke this action
    invoking_actions = []
    for name, config in action_list.items():
        invoke_next = config.get("InvokeNext", [])
        if isinstance(invoke_next, list) and action_name in invoke_next:
            invoking_actions.append(name)
        elif isinstance(invoke_next, dict):
            # Handle conditional invocations
            for condition in ["True", "False"]:
                if condition in invoke_next and action_name in invoke_next[condition]:
                    invoking_actions.append(name)
    
    # Find actions this action invokes
    next_actions = action_list.get(action_name, {}).get("InvokeNext", [])
    
    # Determine position
    is_first = len(invoking_actions) == 0
    is_last = not next_actions
    
    position_type = "middle"
    if is_first and is_last:
        position_type = "only"
    elif is_first:
        position_type = "first"
    elif is_last:
        position_type = "last"
    
    return {
        "is_first": is_first,
        "is_last": is_last,
        "type": position_type
    }

def get_vm_requiring_actions(faasr_payload):
    """
    Get a list of all actions that require VM.
    
    Args:
        faasr_payload: The FaaSr workflow configuration payload
        
    Returns:
        list: Names of actions requiring VM
    """
    vm_actions = []
    
    for name, config in faasr_payload.get("ActionList", {}).items():
        if config.get("RequiresVM", False):
            vm_actions.append(name)
    
    return vm_actions

def is_first_vm_action(faasr_payload, action_name):
    """
    Check if action is the first VM-requiring action in execution flow.
    
    Args:
        faasr_payload: The FaaSr workflow configuration payload
        action_name: Name of the action to check
        
    Returns:
        bool: True if it's the first VM action, False otherwise
    """
    from .detection import action_requires_vm
    
    if not action_requires_vm(faasr_payload, action_name):
        return False
    
    position = get_action_position(faasr_payload, action_name)
    
    if position["is_first"]:
        return True
        
    # If not the first action in workflow, check if it's the first VM-requiring action
    for name, config in faasr_payload.get("ActionList", {}).items():
        # Skip the current action
        if name == action_name:
            continue
            
        # If we find another VM-requiring action that runs before this one, this isn't first
        if config.get("RequiresVM", False):
            position = get_action_position(faasr_payload, name)
            if position["is_first"]:
                return False
    
    return True

def is_last_vm_action(faasr_payload, action_name):
    """
    Check if action is the last VM-requiring action in execution flow.
    
    Args:
        faasr_payload: The FaaSr workflow configuration payload
        action_name: Name of the action to check
        
    Returns:
        bool: True if it's the last VM action, False otherwise
    """
    from .detection import action_requires_vm
    
    if not action_requires_vm(faasr_payload, action_name):
        return False
    
    position = get_action_position(faasr_payload, action_name)
    
    if position["is_last"]:
        return True
        
    # If not the last action in workflow, check if it's the last VM-requiring action
    for name, config in faasr_payload.get("ActionList", {}).items():
        # Skip the current action
        if name == action_name:
            continue
            
        # If we find another VM-requiring action that runs after this one, this isn't last
        if config.get("RequiresVM", False):
            position = get_action_position(faasr_payload, name)
            if position["is_last"]:
                return False
    
    return True