"""
GitHub Actions runner status verification.
"""
import logging
import time
import requests

logger = logging.getLogger("FaaSr_py.vm")


def check_runner_online(repo_owner, repo_name, runner_name, github_token, timeout=300):
    """
    Poll GitHub API to check if self-hosted runner is online.
    
    Args:
        repo_owner: Repository owner (username or org)
        repo_name: Repository name
        runner_name: Name of the runner
        github_token: GitHub PAT
        timeout: Max seconds to wait
        
    Returns:
        bool: True if runner online, False if timeout
    """
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/runners"
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    start_time = time.time()
    check_interval = 10  # Check every 10 seconds
    
    logger.info(f"Polling GitHub API for runner: {runner_name}")
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                runners = data.get("runners", [])
                
                for runner in runners:
                    if runner.get("name") == runner_name:
                        status = runner.get("status")
                        logger.info(f"Runner {runner_name} status: {status}")
                        
                        if status == "online":
                            elapsed = int(time.time() - start_time)
                            logger.info(f"Runner verified online after {elapsed} seconds")
                            return True
                        else:
                            logger.debug(f"Runner status is '{status}', waiting...")
                            break
                else:
                    logger.warning(f"Runner {runner_name} not found in runners list")
            
            elif response.status_code == 401:
                logger.error("GitHub API authentication failed - check PAT token")
                return False
            elif response.status_code == 404:
                logger.error(f"Repository {repo_owner}/{repo_name} not found")
                return False
            else:
                logger.warning(f"GitHub API returned status {response.status_code}")
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"GitHub API request failed: {e}")
        
        # Wait before next check
        time.sleep(check_interval)
    
    # Timeout reached
    elapsed = int(time.time() - start_time)
    logger.warning(f"Runner verification timeout after {elapsed} seconds")
    return False


def extract_runner_name_from_vm_config(vm_config):
    """
    Extract expected runner name from VM configuration.
    
    Args:
        vm_config: VM configuration dict
        
    Returns:
        str: Expected runner name or None
    """
    
    if "RunnerName" in vm_config:
        return vm_config["RunnerName"]
    
    logger.warning("RunnerName not specified in VMConfig - cannot verify runner status")
    return None