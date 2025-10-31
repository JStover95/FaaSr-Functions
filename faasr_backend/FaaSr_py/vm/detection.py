"""
Functions for detecting VM requirements in FaaSr workflows.
"""

import logging

logger = logging.getLogger("FaaSr_py.vm")

def workflow_needs_vm(faasr_payload):
    """
    Check if any function in the workflow requires VM resources.
    
    Args:
        faasr_payload: The FaaSr workflow configuration payload
        
    Returns:
        bool: True if any function requires VM, False otherwise
    """
    # First, verify this is GitHub Actions
    current_action = faasr_payload.get("FunctionInvoke", "")
    if not current_action:
        return False
    
    try:
        current_server = faasr_payload["ActionList"][current_action]["FaaSServer"]
        server_type = faasr_payload["ComputeServers"][current_server]["FaaSType"]
        
        if server_type != "GitHubActions":
            # VM orchestration only supported for GitHub Actions
            return False
    except KeyError:
        logger.warning("Invalid payload structure - cannot determine FaaS type")
        return False
    
    # Check if VMConfig section exists
    if "VMConfig" not in faasr_payload:
        return False
    
    # Check if any function requires VM
    for action_name, action_config in faasr_payload.get("ActionList", {}).items():
        if action_config.get("RequiresVM", False):
            return True
    
    return False

def action_requires_vm(faasr_payload, action_name):
    """
    Check if a specific action requires VM.
    
    Args:
        faasr_payload: The FaaSr workflow configuration payload
        action_name: Name of the action to check
        
    Returns:
        bool: True if the action requires VM, False otherwise
    """
    if action_name not in faasr_payload.get("ActionList", {}):
        return False
    
    return faasr_payload["ActionList"][action_name].get("RequiresVM", False)

def validate_vm_config(vm_config):
    """
    Validate VM configuration parameters.
    
    Args:
        vm_config: The VM configuration object
        
    Returns:
        bool: True if valid, raises ValueError if invalid
    """
    required_fields = ["Provider", "InstanceId", "Region", "AccessKey", "SecretKey"]
    for field in required_fields:
        if field not in vm_config or not vm_config[field]:
            raise ValueError(f"Missing required VM configuration field: {field}")
    
    if vm_config["Provider"] != "AWS":
        raise ValueError(f"Unsupported VM provider: {vm_config['Provider']}")
    
    return True