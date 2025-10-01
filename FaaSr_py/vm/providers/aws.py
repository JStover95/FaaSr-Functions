"""
AWS-specific VM management functions.
"""

import boto3
import logging
import time

logger = logging.getLogger("FaaSr_py.vm")

def start_vm(vm_config):
    """
    Start VM instance based on VMConfig.
    
    Args:
        vm_config: The VM configuration
        
    Returns:
        dict: VM details including instance ID and state
    """
    instance_id = vm_config.get("InstanceId")
    region = vm_config.get("Region", "us-east-1")
    access_key = vm_config.get("AccessKey")
    secret_key = vm_config.get("SecretKey")
    
    if not all([instance_id, region, access_key, secret_key]):
        raise ValueError("Missing required VM configuration parameters")
    
    logger.info(f"Starting VM instance {instance_id} in {region}")
    
    # Create EC2 client
    ec2 = boto3.client(
        'ec2',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    
    # Check current instance state
    try:
        status = check_vm_status(vm_config)
        if status["instance_running"]:
            logger.info(f"Instance {instance_id} is already running")
            return {
                "InstanceId": instance_id,
                "State": "running",
                "Provider": "AWS"
            }
    except Exception as e:
        logger.warning(f"Could not check VM status: {str(e)}")
    
    # Start instance
    try:
        response = ec2.start_instances(InstanceIds=[instance_id])
        
        if response["StartingInstances"]:
            instance_info = response["StartingInstances"][0]
            logger.info(f"Instance {instance_id} starting. Current state: {instance_info['CurrentState']['Name']}")
            
            return {
                "InstanceId": instance_id,
                "State": instance_info["CurrentState"]["Name"],
                "Provider": "AWS"
            }
        else:
            raise ValueError("Failed to start instance - no instances returned")
    except Exception as e:
        raise RuntimeError(f"Failed to start instance {instance_id}: {str(e)}")

def stop_vm(vm_config):
    """
    Stop VM instance based on VMConfig.
    
    Args:
        vm_config: The VM configuration
        
    Returns:
        bool: Success flag
    """
    instance_id = vm_config.get("InstanceId")
    region = vm_config.get("Region", "us-east-1")
    access_key = vm_config.get("AccessKey")
    secret_key = vm_config.get("SecretKey")
    
    if not all([instance_id, region, access_key, secret_key]):
        raise ValueError("Missing required VM configuration parameters")
    
    logger.info(f"Stopping VM instance {instance_id}")
    
    # Create EC2 client
    ec2 = boto3.client(
        'ec2',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    
    # Stop instance
    try:
        response = ec2.stop_instances(InstanceIds=[instance_id])
        
        if response["StoppingInstances"]:
            instance_info = response["StoppingInstances"][0]
            logger.info(f"Instance {instance_id} stopping. Current state: {instance_info['CurrentState']['Name']}")
            return True
        else:
            raise ValueError("Failed to stop instance - no instances returned")
    except Exception as e:
        logger.error(f"Failed to stop instance {instance_id}: {str(e)}")
        return False

def check_vm_status(vm_config):
    """
    Check status of VM instance using AWS API.
    
    Args:
        vm_config: VM configuration including credentials and instance ID
        
    Returns:
        dict: Dictionary with instance_running and status_checks_passed flags
    """
    instance_id = vm_config.get("InstanceId")
    region = vm_config.get("Region", "us-east-1")
    access_key = vm_config.get("AccessKey")
    secret_key = vm_config.get("SecretKey")
    
    if not all([instance_id, region, access_key, secret_key]):
        raise ValueError("Missing required VM configuration parameters")
    
    # Create EC2 client
    ec2 = boto3.client(
        'ec2',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    
    # Get instance status
    response = ec2.describe_instances(InstanceIds=[instance_id])
    
    if not response["Reservations"] or not response["Reservations"][0]["Instances"]:
        raise ValueError(f"Instance not found: {instance_id}")
    
    instance = response["Reservations"][0]["Instances"][0]
    instance_state = instance["State"]["Name"]
    
    # Check if instance is running
    instance_running = (instance_state == "running")
    
    # Check system status (only if running)
    status_checks_passed = False
    if instance_running:
        try:
            status_result = ec2.describe_instance_status(InstanceIds=[instance_id])
            
            if status_result["InstanceStatuses"]:
                status_info = status_result["InstanceStatuses"][0]
                instance_status = status_info["InstanceStatus"]["Status"]
                system_status = status_info["SystemStatus"]["Status"]
                
                # Both instance and system status should be "ok"
                status_checks_passed = (instance_status == "ok" and system_status == "ok")
        except Exception:
            # If status check API fails, assume not ready
            status_checks_passed = False
    
    return {
        "instance_running": instance_running,
        "status_checks_passed": status_checks_passed,
        "instance_state": instance_state
    }

def wait_for_vm_ready(vm_config, vm_details):
    """
    Wait for VM to be ready with status verification.
    
    Args:
        vm_config: VM configuration
        vm_details: VM details from start operation
    """
    max_wait_time = 300  # 5 minutes total
    check_interval = 20  # Check every 20 seconds
    start_time = time.time()
    
    logger.info("Waiting for VM to be ready...")
    
    while True:
        elapsed_time = time.time() - start_time
        
        # Timeout check
        if elapsed_time > max_wait_time:
            logger.warning("VM wait timeout reached - proceeding (runner may not be ready)")
            break
        
        # Check VM status
        try:
            vm_status = check_vm_status(vm_config)
            
            if vm_status["instance_running"] and vm_status["status_checks_passed"]:
                # VM is running and healthy, wait a bit more for GitHub runner service
                logger.info(f"VM is running and healthy after {int(elapsed_time)} seconds")
                
                # Additional wait for GitHub runner service to register
                if elapsed_time < 90:
                    additional_wait = 90 - elapsed_time
                    logger.info(f"Waiting additional {int(additional_wait)} seconds for GitHub runner service...")
                    time.sleep(additional_wait)
                
                logger.info("VM and GitHub runner service should be ready")
                break
            else:
                # VM not ready yet
                status_msg = f"VM Status - Running: {vm_status['instance_running']}, Status Checks: {vm_status['status_checks_passed']}"
                logger.debug(status_msg)
        except Exception as e:
            # If status check fails, continue waiting
            logger.debug(f"Status check failed: {str(e)}")
        
        time.sleep(check_interval)