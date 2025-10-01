"""
Main VM orchestration logic for FaaSr.
"""

import logging
from .detection import action_requires_vm, validate_vm_config
from .position import get_action_position, is_first_vm_action, is_last_vm_action
from .providers import start_vm, stop_vm, wait_for_vm_ready

logger = logging.getLogger("FaaSr_py.vm")

def get_vm_strategy(faasr_payload):
    """
    Get VM orchestration strategy from configuration.
    
    Args:
        faasr_payload: The workflow configuration
        
    Returns:
        str: The VM orchestration strategy
    """
    # Default to simple start/end strategy
    default_strategy = "simple_start_end"
    
    if "VMConfig" not in faasr_payload:
        return default_strategy
    
    vm_config = faasr_payload["VMConfig"]
    strategy = vm_config.get("Strategy", default_strategy)
    return strategy

def orchestrate_vm(faasr_payload):
    """
    Main VM orchestration entry point.
    
    Args:
        faasr_payload: The workflow configuration
        
    Returns:
        dict: Updated workflow configuration
    """
    # Get current action
    current_action = faasr_payload.get("FunctionInvoke", "")
    if not current_action:
        logger.warning("No current action specified in payload")
        return faasr_payload
    
    # Check if the action requires VM resources
    if not action_requires_vm(faasr_payload, current_action):
        logger.debug(f"Action {current_action} does not require VM resources")
        return faasr_payload
    
    # Get VM strategy
    vm_strategy = get_vm_strategy(faasr_payload)
    
    # Dispatch to strategy handler
    if vm_strategy == "simple_start_end":
        return execute_strategy_simple_start_end(faasr_payload)
    elif vm_strategy == "per_function":
        # Future implementation
        logger.warning("Strategy 'per_function' not yet implemented, using 'simple_start_end'")
        return execute_strategy_simple_start_end(faasr_payload)
    elif vm_strategy == "optimized":
        # Future implementation
        logger.warning("Strategy 'optimized' not yet implemented, using 'simple_start_end'")
        return execute_strategy_simple_start_end(faasr_payload)
    else:
        logger.warning(f"Unknown VM strategy: {vm_strategy}, using 'simple_start_end'")
        return execute_strategy_simple_start_end(faasr_payload)

def execute_strategy_simple_start_end(faasr_payload):
    """
    Execute Strategy 1: Simple start/end for existing instance.
    
    Args:
        faasr_payload: The workflow configuration
        
    Returns:
        dict: Updated workflow configuration
    """
    current_action = faasr_payload.get("FunctionInvoke", "")
    
    # Validate VM config before proceeding
    if "VMConfig" not in faasr_payload:
        logger.error("VMConfig not found in workflow configuration")
        return faasr_payload
    
    vm_config = faasr_payload["VMConfig"]
    
    try:
        validate_vm_config(vm_config)
    except ValueError as e:
        logger.error(f"Invalid VM configuration: {str(e)}")
        return faasr_payload
    
    # STEP 1: Start VM if this is the first VM-requiring function
    if is_first_vm_action(faasr_payload, current_action):
        logger.info(f"Action {current_action} is first VM-requiring function - starting VM")
        
        try:
            # Start VM and wait for it to be ready
            vm_details = start_vm(vm_config)
            wait_for_vm_ready(vm_config, vm_details)
            
            logger.info("VM started and ready for GitHub Actions self-hosted runner")
        except Exception as e:
            logger.error(f"Failed to start VM: {str(e)}")
    else:
        logger.info(f"Action {current_action} is not the first VM-requiring function - VM already running")
    
    # STEP 2: Stop VM if this is the last VM-requiring function
    if is_last_vm_action(faasr_payload, current_action):
        logger.info(f"Action {current_action} is last VM-requiring function - stopping VM after completion")
        
        try:
            # Stop VM after function completes
            stop_vm(vm_config)
            
            logger.info("VM stopped after last VM-requiring function")
        except Exception as e:
            logger.error(f"Failed to stop VM: {str(e)}")
    else:
        logger.info(f"Action {current_action} is not the last VM-requiring function - keeping VM running")
    
    return faasr_payload