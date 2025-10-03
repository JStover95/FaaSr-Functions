"""
Built-in function for VM startup.
Starts VM before workflow execution begins.
"""
import logging
import os
import time

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
        from FaaSr_py.vm.github_runner import check_runner_online, extract_runner_name_from_vm_config
        
        validate_vm_config(vm_config)
        
        # Check if we'll be polling GitHub
        github_token = os.getenv("GH_PAT")
        skip_fixed_wait = github_token is not None

        logger.info("Checking current VM status...")
        try:
            vm_status = check_vm_status(vm_config)
            if vm_status["instance_running"] and vm_status["status_checks_passed"]:
                logger.info(f"VM instance {vm_config['InstanceId']} is already running and healthy")
            else:
                logger.info(f"Starting VM instance {vm_config['InstanceId']} in region {vm_config['Region']}")
                vm_details = start_vm(vm_config)
                
                logger.info("Waiting for VM to be ready...")
                wait_for_vm_ready(vm_config, vm_details, skip_runner_wait=skip_fixed_wait)
        except Exception as e:
            logger.warning(f"Could not check VM status: {e}, will attempt start")
            vm_details = start_vm(vm_config)
            wait_for_vm_ready(vm_config, vm_details, skip_runner_wait=skip_fixed_wait)
    
    # Verify GitHub runner is online
        # Verify GitHub runner is online
        github_token = os.getenv("GH_PAT")
        if github_token:
            logger.info("GitHub PAT found - will verify runner status")
            
            # Get repo info from FaaSr payload
            current_action = faasr_payload.get("FunctionInvoke")
            if current_action:
                action_config = faasr_payload["ActionList"][current_action]
                server_name = action_config.get("FaaSServer")
                
                if server_name:
                    server_config = faasr_payload["ComputeServers"][server_name]
                    repo_owner = server_config.get("UserName")
                    repo_name = server_config.get("ActionRepoName")
                    
                    # Get runner name
                    runner_name = extract_runner_name_from_vm_config(vm_config)
                    
                    if repo_owner and repo_name and runner_name:
                        logger.info(f"Verifying runner {runner_name} in {repo_owner}/{repo_name}")
                        
                        runner_online = check_runner_online(
                            repo_owner=repo_owner,
                            repo_name=repo_name,
                            runner_name=runner_name,
                            github_token=github_token,
                            timeout=300  # 5 minutes max
                        )
                        
                        if runner_online:
                            logger.info("GitHub runner verified online - workflow can proceed")
                        else:
                            logger.warning("Runner verification timed out - proceeding anyway")
                    else:
                        logger.warning("Missing repo/runner info - skipping verification")
        else:
            logger.warning("GH_PAT not found - skipping runner verification, waiting 90 seconds")
            time.sleep(90)
        
        logger.info("VM started and ready for workflow execution")
        return True
        
    except Exception as e:
        logger.error(f"Failed to start VM: {e}")
        raise