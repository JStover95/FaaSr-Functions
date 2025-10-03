"""
Built-in function for VM startup.
Starts VM before workflow execution begins.
"""
import logging
import os

logger = logging.getLogger("FaaSr_py.builtin")

def vm_start(faasr_payload):
    """
    Start VM before workflow execution.
    This is automatically injected for VM workflows.
    
    Args:
        faasr_payload: FaaSrPayload object
        
    Returns:
        True on success
    """
    logger.info("VM start action executing")
    
    if "VMConfig" not in faasr_payload:
        logger.error("No VMConfig found in workflow")
        raise ValueError("VMConfig required for VM start action")
    
    vm_config = faasr_payload["VMConfig"]
    
    # Add credentials from environment
    vm_name = vm_config.get("Name")
    if not vm_name:
        raise ValueError("VMConfig.Name is required")
    
    access_key_env = f"{vm_name}_AccessKey"
    secret_key_env = f"{vm_name}_SecretKey"
    
    access_key = os.getenv(access_key_env)
    secret_key = os.getenv(secret_key_env)
    
    if not access_key or not secret_key:
        raise ValueError(f"VM credentials not found: {access_key_env}, {secret_key_env}")
    
    vm_config["AccessKey"] = access_key
    vm_config["SecretKey"] = secret_key
    
    try:
        from FaaSr_py.vm.detection import validate_vm_config
        from FaaSr_py.vm.providers import start_vm, wait_for_vm_ready
        from FaaSr_py.vm.providers.aws import check_vm_status
        
        validate_vm_config(vm_config)
        
        # Check if already running (idempotent)
        logger.info("Checking current VM status...")
        try:
            vm_status = check_vm_status(vm_config)
            if vm_status["instance_running"]:
                logger.info(f"VM instance {vm_config['InstanceId']} is already running")
                if vm_status["status_checks_passed"]:
                    logger.info("VM is healthy - workflow can proceed")
                    return True
                else:
                    logger.info("VM running but status checks pending - will wait")
        except Exception as e:
            logger.warning(f"Could not check VM status: {e}, will attempt start")
        
        logger.info(f"Starting VM instance {vm_config['InstanceId']} in region {vm_config['Region']}")
        vm_details = start_vm(vm_config)
        
        logger.info("Waiting for VM and GitHub runner to be ready...")
        wait_for_vm_ready(vm_config, vm_details)
        
        logger.info("VM started and ready for workflow execution")
        return True
        
    except Exception as e:
        logger.error(f"Failed to start VM: {e}")
        raise