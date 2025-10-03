"""
Built-in function for VM shutdown.
Stops VM after all workflow actions complete.
"""
import logging
import os

logger = logging.getLogger("FaaSr_py.builtin")

def vm_stop(faasr_payload):
    """
    Stop VM after workflow completion.
    This is automatically injected for VM workflows.
    
    Args:
        faasr_payload: FaaSrPayload object
        
    Returns:
        True on success
    """
    logger.info("VM stop action executing")
    
    if "VMConfig" not in faasr_payload:
        logger.warning("No VMConfig found - skipping VM stop")
        return True
    
    vm_config = faasr_payload["VMConfig"]
    
    # Add credentials from environment
    vm_name = vm_config.get("Name")
    if vm_name:
        access_key_env = f"{vm_name}_AccessKey"
        secret_key_env = f"{vm_name}_SecretKey"
        
        vm_config["AccessKey"] = os.getenv(access_key_env)
        vm_config["SecretKey"] = os.getenv(secret_key_env)
    
    try:
        from FaaSr_py.vm.providers import stop_vm
        from FaaSr_py.vm.aws import check_vm_status
        
        # Check if VM is actually running before attempting stop
        try:
            vm_status = check_vm_status(vm_config)
            if not vm_status["instance_running"]:
                logger.info(f"VM instance {vm_config['InstanceId']} is already stopped")
                return True
        except Exception as e:
            logger.warning(f"Could not check VM status: {e}, will attempt stop anyway")
        
        logger.info(f"Stopping VM instance {vm_config['InstanceId']}")
        stop_vm(vm_config)
        logger.info("VM stopped successfully")
        
        return True
    except Exception as e:
        logger.error(f"Failed to stop VM: {e}")
        # Don't fail workflow if cleanup fails
        logger.warning("VM stop failed but workflow will complete")
        return True