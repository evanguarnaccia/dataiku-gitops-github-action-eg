import os
import subprocess
import traceback
import dataikuapi
import urllib3

# Disable warnings for unverified HTTPS requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
DATAIKU_API_TOKEN_DEV = os.getenv('DATAIKU_API_TOKEN_DEV')
DATAIKU_API_TOKEN_STAGING = os.getenv('DATAIKU_API_TOKEN_STAGING')
DATAIKU_API_TOKEN_PROD = os.getenv('DATAIKU_API_TOKEN_PROD')

DATAIKU_INSTANCE_DEV_URL = os.getenv('DATAIKU_INSTANCE_DEV_URL')
DATAIKU_INSTANCE_STAGING_URL = os.getenv('DATAIKU_INSTANCE_STAGING_URL')
DATAIKU_INSTANCE_PROD_URL = os.getenv('DATAIKU_INSTANCE_PROD_URL')

DATAIKU_PROJECT_KEY = os.getenv('DATAIKU_PROJECT_KEY')

# Hardcoded Infra IDs (Ensure these match your IDs in Project Deployer)
DATAIKU_INFRA_ID_STAGING = "staging"  
DATAIKU_INFRA_ID_PROD = "prod"        

RUN_TESTS_ONLY = os.getenv('RUN_TESTS_ONLY', 'false').lower() == 'true'
PYTHON_SCRIPT = os.getenv('PYTHON_SCRIPT', 'tests.py')
CLIENT_CERTIFICATE = os.getenv('CLIENT_CERTIFICATE', None)

# --- Client Initialization ---
# Helper to create clients safely
def get_client(url, token):
    if not url or not token:
        return None
    return dataikuapi.DSSClient(url, token, no_check_certificate=True, client_certificate=CLIENT_CERTIFICATE)

client_dev = get_client(DATAIKU_INSTANCE_DEV_URL, DATAIKU_API_TOKEN_DEV)
# client_staging/prod initialized only if needed in tests, usually passed as params

# --- Helper Functions ---

def get_git_sha():
    """Gets the short SHA of the current Git commit."""
    try:
        # Fixed: Added the missing subprocess call
        result = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except Exception:
        # Fallback for local testing if git isn't available
        return "manual-test"

def generate_bundle_id(commit_id):
    """Generates a consistent Bundle ID based on the Git SHA."""
    # Using the SHA ensures the ID is the same across different workflow runs
    return f"bundle_{commit_id}"

def run_tests(script_path, instance_url, api_key, project_key):
    """Runs a separate python test script."""
    print(f"Running tests against {instance_url}...")
    try:
        # Fixed: Added missing subprocess call
        # Passes connection details to the test script via environment variables
        env = os.environ.copy()
        env['DSS_HOST'] = instance_url
        env['DSS_API_KEY'] = api_key
        env['DSS_PROJECT'] = project_key
        
        result = subprocess.run(['python', script_path], env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Tests Passed!")
            return True
        else:
            print(f"Tests Failed:\n{result.stderr}\n{result.stdout}")
            return False
    except Exception as e:
        print(f"Error running tests: {e}")
        return False

# --- Core Deployment Logic ---

def deploy(infra_id):
    """
    Deploy to a specific infrastructure. 
    Handles Bundle creation (if missing) and Deployment creation/update.
    """
    try:
        commit_id = get_git_sha()
        bundle_id = generate_bundle_id(commit_id)
        
        deployer = client_dev.get_projectdeployer()
        
        # 1. CHECK IF BUNDLE EXISTS
        # We list bundles in the Deployer to see if this Git SHA has already been exported.
        # This prevents the "Bundle already exists" error when re-running workflows.
        bundle_exists = False
        try:
            # Check the project in the deployer
            deployer_project = deployer.get_project(DATAIKU_PROJECT_KEY)
            bundles = deployer_project.list_bundles()
            for b in bundles:
                if b['bundleId'] == bundle_id:
                    bundle_exists = True
                    break
        except:
            # Project might not exist in deployer yet, which is fine
            pass

        if bundle_exists:
            print(f"Bundle '{bundle_id}' already exists in Deployer. Skipping export.")
        else:
            print(f"Bundle '{bundle_id}' not found. Exporting from Design node...")
            project = client_dev.get_project(DATAIKU_PROJECT_KEY)
            
            # Export (handle race condition if it exists in Project but not Deployer)
            try:
                project.export_bundle(bundle_id)
            except Exception as e:
                if "already exists" in str(e):
                    print(f"Bundle existed in Project but not Deployer. Proceeding to publish.")
                else:
                    raise e
            
            # Publish to Deployer
            project.publish_bundle(bundle_id)
            print(f"Published bundle '{bundle_id}' to Deployer.")

        # 2. DEPLOY
        # We append the infra_id to the deployment ID.
        # This creates TWO separate deployments: "deploy_bundleX_staging" and "deploy_bundleX_prod"
        deployment_id = f"deploy_{DATAIKU_PROJECT_KEY}_{infra_id}"
        
        print(f"Preparing deployment '{deployment_id}' on infra '{infra_id}'...")
        
        deployment = None
        try:
            deployment = deployer.get_deployment(deployment_id)
            # If deployment exists, update it to use the new bundle
            settings = deployment.get_settings()
            settings.get_raw()['bundleId'] = bundle_id
            settings.save()
            print(f"Updated existing deployment to use bundle {bundle_id}")
        except:
            # If deployment doesn't exist, create it
            print("Creating new deployment...")
            deployment = deployer.create_deployment(
                deployment_id=deployment_id,
                project_key=DATAIKU_PROJECT_KEY,
                infra_id=infra_id,
                bundle_id=bundle_id
            )
        
        # 3. ACTIVATE
        print(f"Activating deployment on {infra_id}...")
        update = deployment.start_update()
        update.wait_for_result()
        print(f"SUCCESS: Deployed {bundle_id} to {infra_id}")

    except Exception as e:
        print(f"Failed to deploy: {str(e)}")
        raise e

# --- Main Workflow ---

def main():
    try:
        print("--- Starting GitOps Workflow ---")
        
        # 1. Deploy to Staging
        print(f"\n[Phase 1] Deploying to Staging ({DATAIKU_INFRA_ID_STAGING})...")
        deploy(DATAIKU_INFRA_ID_STAGING)

        # 2. Run Tests on Staging
        print(f"\n[Phase 2] Running Tests on Staging...")
        tests_passed = run_tests(PYTHON_SCRIPT, DATAIKU_INSTANCE_STAGING_URL, DATAIKU_API_TOKEN_STAGING, DATAIKU_PROJECT_KEY)
        
        if not tests_passed:
            print("Tests failed in staging. Aborting.")
            sys.exit(1)
            
        print("Tests passed in staging.")
        
        if RUN_TESTS_ONLY:
            print("RUN_TESTS_ONLY=true. Skipping Prod deployment.")
            sys.exit(0)

        # 3. Deploy to Production
        print(f"\n[Phase 3] Deploying to Production ({DATAIKU_INFRA_ID_PROD})...")
        # Because we fixed the logic, this will reuse the existing bundle 
        # instead of trying to create it again.
        deploy(DATAIKU_INFRA_ID_PROD)
        
        # 4. Optional: Run Smoke Tests on Prod
        print(f"\n[Phase 4] Verifying Production...")
        prod_tests_passed = run_tests(PYTHON_SCRIPT, DATAIKU_INSTANCE_PROD_URL, DATAIKU_API_TOKEN_PROD, DATAIKU_PROJECT_KEY)
        
        if prod_tests_passed:
            print("Deployment and verification successful!")
        else:
            print("WARNING: Production tests failed after deployment.")
            sys.exit(1)

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
