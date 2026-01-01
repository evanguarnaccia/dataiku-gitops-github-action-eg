import hashlib
import os
import random
import string
import subprocess
import sys
import time
import traceback
from datetime import datetime
from time import sleep

import dataikuapi
import urllib3

# Disable warnings for unverified HTTPS requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Access environment variables
DATAIKU_API_TOKEN_DEV = os.getenv('DATAIKU_API_TOKEN_DEV')
DATAIKU_API_TOKEN_STAGING = os.getenv('DATAIKU_API_TOKEN_STAGING')
DATAIKU_API_TOKEN_PROD = os.getenv('DATAIKU_API_TOKEN_PROD')
DATAIKU_INSTANCE_DEV_URL = os.getenv('DATAIKU_INSTANCE_DEV_URL')
DATAIKU_INSTANCE_STAGING_URL = os.getenv('DATAIKU_INSTANCE_STAGING_URL')
DATAIKU_INSTANCE_PROD_URL = os.getenv('DATAIKU_INSTANCE_PROD_URL')
DATAIKU_PROJECT_KEY = os.getenv('DATAIKU_PROJECT_KEY')
#DATAIKU_INFRA_ID_STAGING = os.getenv('DATAIKU_INFRA_ID_STAGING')
DATAIKU_INFRA_ID_STAGING = "staging"
#DATAIKU_INFRA_ID_PROD = os.getenv('DATAIKU_INFRA_ID_PROD')
DATAIKU_INFRA_ID_PROD = "prod"
RUN_TESTS_ONLY = os.getenv('RUN_TESTS_ONLY', 'false').lower() == 'true'
PYTHON_SCRIPT = os.getenv('PYTHON_SCRIPT', 'tests.py')
CLIENT_CERTIFICATE = os.getenv('CLIENT_CERTIFICATE', None)

# Create Dataiku clients
client_dev = dataikuapi.DSSClient(DATAIKU_INSTANCE_DEV_URL, DATAIKU_API_TOKEN_DEV, no_check_certificate=True, client_certificate=CLIENT_CERTIFICATE)
client_staging = dataikuapi.DSSClient(DATAIKU_INSTANCE_STAGING_URL, DATAIKU_API_TOKEN_STAGING, no_check_certificate=True, client_certificate=CLIENT_CERTIFICATE)
client_prod = dataikuapi.DSSClient(DATAIKU_INSTANCE_PROD_URL, DATAIKU_API_TOKEN_PROD, no_check_certificate=True, client_certificate=CLIENT_CERTIFICATE)

def get_commit_id():
    result = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True)
    return result.stdout.strip()

def generate_bundle_id(commit_id):
    """
    Generates a unique Bundle ID using Git SHA + Timestamp.
    Example: bundle_a1b2c3d4_2023-10-27_14-30-05
    """
    # 1. Get current time in a safe filename format (YYYY-MM-DD_HH-MM-SS)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # 2. Combine shortened Commit ID with the Timestamp
    # We keep the commit ID for traceability, but the timestamp ensures uniqueness
    return f"bundle_{commit_id[:8]}_{timestamp}"

def export_bundle(client, project_key, bundle_id, release_notes=None):
    project = client.get_project(project_key)
    project.export_bundle(bundle_id, release_notes)
    return bundle_id


def run_tests(script_path, instance_url, api_key, project_key):
    """Run pytest with environment variables for Dataiku configuration."""
    env = os.environ.copy()
    env.update({
        'DATAIKU_INSTANCE_URL': instance_url,
        'DATAIKU_API_KEY': api_key,
        'DATAIKU_PROJECT_KEY': project_key
    })
    
    result = subprocess.run([
        'pytest',
        '-v',
        '-s',
        script_path,
        '--no-header',  # Minimize output noise
        '--tb=short'    # Shorter traceback format
    ], env=env, capture_output=True, text=True)
    
    # Print test output for visibility
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    
    return result.returncode == 0

def get_dataiku_latest_commit(client, project_key):
    """Get the latest commit SHA from Dataiku project."""
    project = client.get_project(project_key)
    project_git = project.get_project_git()
    
    # Get the git log to find the latest commit
    log = project_git.log(count=1)  # Get only the most recent commit
    
    if not log or 'entries' not in log or not log['entries']:
        raise ValueError("No commit history found in Dataiku project")
    
    entry = log['entries'][0]
    if 'commit' not in entry:
        raise ValueError(f"No commit field found in log entry: {entry}")
        
    return entry['commit']

def sync_dataiku_to_git(client, project_key):
    """Push Dataiku changes to Git."""
    project = client.get_project(project_key).get_project_git()
    return project.push()

def get_git_sha():
    """Get commits from origin/master."""
    # First fetch to ensure we have latest
    subprocess.run(['git', 'fetch', 'origin', 'master'], capture_output=True)
    
    # Get the first commit from origin/master
    result = subprocess.run(['git', 'log', 'origin/master', '-n', '1', '--pretty=format:%H'], capture_output=True, text=True)
    if result.returncode != 0:
        raise ValueError("Failed to get commit from origin/master")
    
    return result.stdout.strip()

def deploy(infra_id):
    """Deploy to production using bundle and deployer."""
    try:
        commit_id = get_git_sha()
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        bundle_id = generate_bundle_id(commit_id)
        project = client_dev.get_project(DATAIKU_PROJECT_KEY)
        project.export_bundle(bundle_id)
        
        # Publish the bundle to the deployer
        project.publish_bundle(bundle_id)
        
        # Get the deployer from the client and deploy
        deployer = client_dev.get_projectdeployer()
        deployment = deployer.create_deployment(
            deployment_id=f"deploy_{bundle_id}",
            project_key=DATAIKU_PROJECT_KEY,
            infra_id=infra_id,
            bundle_id=bundle_id
        )
        update = deployment.start_update()
        update.wait_for_result()
        
        print(f"Successfully deployed bundle {bundle_id} to infra {infra_id}")
        
    except Exception as e:
        print(f"Failed to deploy: {str(e)}")
        raise e

def main():
    try:
        #dataiku_sha = get_dataiku_latest_commit(client_dev, DATAIKU_PROJECT_KEY)
        #git_sha = get_git_sha()
        #if dataiku_sha != git_sha:
        #    print(f"Dataiku commit SHA ({dataiku_sha}) doesn't match Git SHA ({git_sha})")
        #    sync_dataiku_to_git(client_dev, DATAIKU_PROJECT_KEY)
        #    print("Pushed Dataiku changes to Git. Restarting process.")
        #    sys.exit(0)

        deploy(DATAIKU_INFRA_ID_STAGING)

        # Run tests on Staging instance
        if run_tests(PYTHON_SCRIPT, DATAIKU_INSTANCE_STAGING_URL, DATAIKU_API_TOKEN_STAGING, DATAIKU_PROJECT_KEY):
            if RUN_TESTS_ONLY:
                print("Tests passed in staging. Skipping deployment to production.")
            else:
                print("Tests passed in staging. Deploying to production.")
                
                # Replace bundle import/export with deployment
                deploy(DATAIKU_INFRA_ID_PROD)
                
                # Run tests on Prod instance
                if run_tests(PYTHON_SCRIPT, DATAIKU_INSTANCE_PROD_URL, DATAIKU_API_TOKEN_PROD, DATAIKU_PROJECT_KEY):
                    print("Deployment and tests successful in production.")
                else:
                    print("Tests failed in production.")
                    # Note: With this approach, rollback needs to be handled through Dataiku's deployment feature
                    sys.exit(1)
        else:
            print("Tests failed in staging.")
            sys.exit(1)

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()  # Print the stack trace
        sys.exit(1)

if __name__ == '__main__':
    main() 
