"""
Built-in function for VM readiness verification.
Waits for VM to be fully operational before dispatching jobs to it.
"""
import logging
import os

logger = logging.getLogger("FaaSr_py.builtin")

def vm_poll(faasr_payload):
    """
    Poll VM until ready and GitHub runner online.
    This runs before each VM-requiring action.
    
    Args:
        faasr_payload: FaaSrPayload object
        
    Returns:
        True when VM ready
    """
    logger.info("VM poll action executing - verifying VM readiness")
    
    if "VMConfig" not in faasr_payload:
        logger.error("No VMConfig found in workflow")
        raise ValueError("VMConfig required for VM poll action")
    
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
        from FaaSr_py.vm.providers import wait_for_vm_ready
        from FaaSr_py.vm.github_runner import check_runner_online, extract_runner_name_from_vm_config
        
        validate_vm_config(vm_config)
        
        # Check GitHub PAT availability
        github_token = os.getenv("GH_PAT")
        skip_fixed_wait = github_token is not None
        
        logger.info("Waiting for VM to be ready...")
        # Pass empty vm_details since we're not starting, just polling
        wait_for_vm_ready(vm_config, vm_details=None, skip_runner_wait=skip_fixed_wait)
        
        # Verify GitHub runner
        if github_token:
            logger.info("GitHub PAT found - will verify runner status")
            
            # Get repo info from payload
            current_action = faasr_payload.get("FunctionInvoke")
            if current_action:
                action_config = faasr_payload["ActionList"][current_action]
                server_name = action_config.get("FaaSServer")
                
                if server_name:
                    server_config = faasr_payload["ComputeServers"][server_name]
                    repo_owner = server_config.get("UserName")
                    repo_name = server_config.get("ActionRepoName")
                    
                    runner_name = extract_runner_name_from_vm_config(vm_config)
                    
                    if repo_owner and repo_name and runner_name:
                        logger.info(f"Verifying runner {runner_name} in {repo_owner}/{repo_name}")
                        
                        runner_online = check_runner_online(
                            repo_owner=repo_owner,
                            repo_name=repo_name,
                            runner_name=runner_name,
                            github_token=github_token,
                            timeout=300
                        )
                        
                        if runner_online:
                            logger.info("GitHub runner verified online - action can proceed")
                        else:
                            logger.error("Runner verification timed out")
                            raise RuntimeError("GitHub runner not available")
                    else:
                        logger.warning("Missing repo/runner info - cannot verify")
        else:
            logger.warning("GH_PAT not found - cannot verify runner status")
        
        logger.info("VM is ready for workflow execution")
        return True
        
    except Exception as e:
        logger.error(f"Failed to verify VM readiness: {e}")
        raise