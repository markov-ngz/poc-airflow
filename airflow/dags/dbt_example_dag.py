# AI generated to set as a lookup

from datetime import datetime, timedelta
from pathlib import Path
import os
import json
import shutil
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSUploadOperator
from airflow.providers.google.cloud.hooks.secret_manager import SecretManagerHook
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

# DAG Configuration
DEFAULT_ARGS = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Environment-specific configurations
DBT_PROJECT_CONFIG = {
    'project_source': Variable.get("dbt_project_source", "git"),  # 'git', 'volume', or 'gcs'
    'git_repo_url': Variable.get("dbt_git_repo_url", ""),
    'git_branch': Variable.get("dbt_git_branch", "main"),
    'volume_path': Variable.get("dbt_volume_path", "/opt/airflow/dbt_projects"),
    'gcs_bucket': Variable.get("dbt_gcs_bucket", ""),
    'gcs_project_path': Variable.get("dbt_gcs_project_path", "dbt_projects/"),
    'project_name': Variable.get("dbt_project_name", "my_dbt_project"),
    'profiles_dir': '/tmp/dbt_profiles',
    'artifacts_dir': '/tmp/dbt_artifacts',
    'target_env': Variable.get("dbt_target_env", "dev"),
}

def setup_dbt_credentials(**context) -> Dict[str, str]:
    """
    Setup dbt credentials from various sources (Airflow Connections, GCP Secret Manager, etc.)
    Returns paths to credential files and profile configuration
    """
    profiles_dir = Path(DBT_PROJECT_CONFIG['profiles_dir'])
    profiles_dir.mkdir(parents=True, exist_ok=True)
    
    # Get database connection details from Airflow Connection
    try:
        # Example for PostgreSQL/Redshift - adapt based on your warehouse
        db_conn = BaseHook.get_connection('dbt_warehouse_conn')
        
        # Setup profiles.yml
        profile_config = {
            DBT_PROJECT_CONFIG['project_name']: {
                'target': DBT_PROJECT_CONFIG['target_env'],
                'outputs': {
                    DBT_PROJECT_CONFIG['target_env']: {
                        'type': db_conn.conn_type or 'postgres',
                        'host': db_conn.host,
                        'user': db_conn.login,
                        'password': db_conn.password,
                        'port': db_conn.port or 5432,
                        'dbname': db_conn.schema,
                        'schema': Variable.get("dbt_target_schema", "analytics"),
                        'threads': 4,
                        'keepalives_idle': 0,
                    }
                }
            }
        }
        
    except Exception as e:
        print(f"Failed to get Airflow connection, trying GCP Secret Manager: {e}")
        
        # Alternative: Use GCP Secret Manager
        try:
            secret_hook = SecretManagerHook()
            db_credentials = json.loads(
                secret_hook.get_secret(
                    secret_id='dbt-warehouse-credentials',
                    project_id=Variable.get("gcp_project_id")
                )
            )
            
            profile_config = {
                DBT_PROJECT_CONFIG['project_name']: {
                    'target': DBT_PROJECT_CONFIG['target_env'],
                    'outputs': {
                        DBT_PROJECT_CONFIG['target_env']: {
                            **db_credentials,
                            'schema': Variable.get("dbt_target_schema", "analytics"),
                            'threads': 4,
                        }
                    }
                }
            }
        except Exception as secret_e:
            raise AirflowException(f"Failed to retrieve credentials: {secret_e}")
    
    # Write profiles.yml
    profiles_file = profiles_dir / 'profiles.yml'
    with open(profiles_file, 'w') as f:
        import yaml
        yaml.dump(profile_config, f, default_flow_style=False)
    
    # Setup additional credential files if needed (e.g., service account keys)
    credential_files = {}
    
    try:
        # Example: GCP Service Account Key
        if Variable.get("gcp_service_account_key", None):
            sa_key_path = profiles_dir / 'gcp_service_account.json'
            with open(sa_key_path, 'w') as f:
                json.dump(json.loads(Variable.get("gcp_service_account_key")), f)
            credential_files['GOOGLE_APPLICATION_CREDENTIALS'] = str(sa_key_path)
            
    except Exception as e:
        print(f"Warning: Could not setup additional credentials: {e}")
    
    return {
        'profiles_dir': str(profiles_dir),
        'credential_files': credential_files,
        'target': DBT_PROJECT_CONFIG['target_env']
    }

def setup_dbt_project(**context) -> str:
    """
    Download or setup dbt project from various sources
    Returns the path to the dbt project directory
    """
    project_dir = f"/tmp/dbt_project_{context['run_id']}"
    
    if DBT_PROJECT_CONFIG['project_source'] == 'git':
        # Clone from Git repository
        git_url = DBT_PROJECT_CONFIG['git_repo_url']
        git_branch = DBT_PROJECT_CONFIG['git_branch']
        
        clone_command = f"""
        git clone --branch {git_branch} --single-branch {git_url} {project_dir}
        """
        
        import subprocess
        result = subprocess.run(clone_command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise AirflowException(f"Git clone failed: {result.stderr}")
            
    elif DBT_PROJECT_CONFIG['project_source'] == 'volume':
        # Copy from mounted volume
        volume_path = Path(DBT_PROJECT_CONFIG['volume_path'])
        if not volume_path.exists():
            raise AirflowException(f"Volume path does not exist: {volume_path}")
        
        shutil.copytree(volume_path, project_dir)
        
    elif DBT_PROJECT_CONFIG['project_source'] == 'gcs':
        # Download from GCS bucket
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        
        gcs_hook = GCSHook()
        bucket_name = DBT_PROJECT_CONFIG['gcs_bucket']
        source_path = DBT_PROJECT_CONFIG['gcs_project_path']
        
        # Create project directory
        Path(project_dir).mkdir(parents=True, exist_ok=True)
        
        # List and download all files in the project path
        objects = gcs_hook.list(bucket_name, prefix=source_path)
        for obj in objects:
            if not obj.endswith('/'):  # Skip directories
                local_file_path = Path(project_dir) / obj.replace(source_path, '').lstrip('/')
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                gcs_hook.download(bucket_name, obj, str(local_file_path))
                
    else:
        raise AirflowException(f"Unknown project source: {DBT_PROJECT_CONFIG['project_source']}")
    
    return project_dir

def run_dbt_commands(project_dir: str, profiles_dir: str, target: str, credential_files: Dict[str, str]) -> Dict[str, Any]:
    """
    Execute dbt commands and return run metadata
    """
    import subprocess
    import json
    from datetime import datetime
    
    # Set environment variables for credentials
    env = os.environ.copy()
    env.update(credential_files)
    env['DBT_PROFILES_DIR'] = profiles_dir
    
    # Change to project directory
    original_cwd = os.getcwd()
    os.chdir(project_dir)
    
    results = {}
    
    try:
        # dbt debug to verify setup
        debug_result = subprocess.run(
            ['dbt', 'debug', '--target', target],
            capture_output=True,
            text=True,
            env=env
        )
        results['debug'] = {
            'returncode': debug_result.returncode,
            'stdout': debug_result.stdout,
            'stderr': debug_result.stderr
        }
        
        if debug_result.returncode != 0:
            raise AirflowException(f"dbt debug failed: {debug_result.stderr}")
        
        # dbt deps to install dependencies
        deps_result = subprocess.run(
            ['dbt', 'deps', '--target', target],
            capture_output=True,
            text=True,
            env=env
        )
        results['deps'] = {
            'returncode': deps_result.returncode,
            'stdout': deps_result.stdout,
            'stderr': deps_result.stderr
        }
        
        # dbt run
        run_result = subprocess.run(
            ['dbt', 'run', '--target', target],
            capture_output=True,
            text=True,
            env=env
        )
        results['run'] = {
            'returncode': run_result.returncode,
            'stdout': run_result.stdout,
            'stderr': run_result.stderr
        }
        
        if run_result.returncode != 0:
            raise AirflowException(f"dbt run failed: {run_result.stderr}")
        
        # dbt test
        test_result = subprocess.run(
            ['dbt', 'test', '--target', target],
            capture_output=True,
            text=True,
            env=env
        )
        results['test'] = {
            'returncode': test_result.returncode,
            'stdout': test_result.stdout,
            'stderr': test_result.stderr
        }
        
        # Generate docs
        docs_generate_result = subprocess.run(
            ['dbt', 'docs', 'generate', '--target', target],
            capture_output=True,
            text=True,
            env=env
        )
        results['docs_generate'] = {
            'returncode': docs_generate_result.returncode,
            'stdout': docs_generate_result.stdout,
            'stderr': docs_generate_result.stderr
        }
        
    finally:
        os.chdir(original_cwd)
    
    return results

def extract_and_save_artifacts(project_dir: str, **context) -> str:
    """
    Extract dbt artifacts and save them to storage
    """
    artifacts_dir = Path(DBT_PROJECT_CONFIG['artifacts_dir'])
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    # Paths to dbt artifacts
    target_dir = Path(project_dir) / 'target'
    
    artifacts = {
        'manifest.json': target_dir / 'manifest.json',
        'run_results.json': target_dir / 'run_results.json',
        'catalog.json': target_dir / 'catalog.json',
        'index.html': target_dir / 'index.html',  # docs
        'graph.gpickle': target_dir / 'graph.gpickle',
    }
    
    # Copy artifacts to staging directory
    run_id = context['run_id']
    timestamp = context['execution_date'].strftime('%Y%m%d_%H%M%S')
    run_artifacts_dir = artifacts_dir / f"run_{run_id}_{timestamp}"
    run_artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    copied_artifacts = {}
    for artifact_name, artifact_path in artifacts.items():
        if artifact_path.exists():
            dest_path = run_artifacts_dir / artifact_name
            shutil.copy2(artifact_path, dest_path)
            copied_artifacts[artifact_name] = str(dest_path)
            print(f"Copied {artifact_name} to {dest_path}")
    
    # Create run metadata
    metadata = {
        'run_id': run_id,
        'execution_date': context['execution_date'].isoformat(),
        'dag_id': context['dag'].dag_id,
        'project_name': DBT_PROJECT_CONFIG['project_name'],
        'target_env': DBT_PROJECT_CONFIG['target_env'],
        'artifacts': copied_artifacts,
        'project_source': DBT_PROJECT_CONFIG['project_source']
    }
    
    metadata_file = run_artifacts_dir / 'run_metadata.json'
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    return str(run_artifacts_dir)

def dbt_pipeline(**context):
    """
    Main dbt pipeline orchestrator
    """
    # Setup credentials
    print("Setting up dbt credentials...")
    cred_info = setup_dbt_credentials(**context)
    
    # Setup project
    print("Setting up dbt project...")
    project_dir = setup_dbt_project(**context)
    
    # Run dbt commands
    print("Running dbt commands...")
    dbt_results = run_dbt_commands(
        project_dir=project_dir,
        profiles_dir=cred_info['profiles_dir'],
        target=cred_info['target'],
        credential_files=cred_info['credential_files']
    )
    
    # Extract artifacts
    print("Extracting and saving artifacts...")
    artifacts_path = extract_and_save_artifacts(project_dir, **context)
    
    # Store results in XCom for downstream tasks
    context['task_instance'].xcom_push(key='dbt_results', value=dbt_results)
    context['task_instance'].xcom_push(key='artifacts_path', value=artifacts_path)
    context['task_instance'].xcom_push(key='project_dir', value=project_dir)
    
    print(f"dbt pipeline completed successfully!")
    print(f"Artifacts saved to: {artifacts_path}")
    
    return {
        'status': 'success',
        'artifacts_path': artifacts_path,
        'project_dir': project_dir
    }

def cleanup_temp_files(**context):
    """
    Clean up temporary files created during the run
    """
    task_instance = context['task_instance']
    
    # Get paths from previous task
    project_dir = task_instance.xcom_pull(key='project_dir', task_ids='run_dbt_pipeline')
    
    # Clean up project directory
    if project_dir and Path(project_dir).exists():
        shutil.rmtree(project_dir)
        print(f"Cleaned up project directory: {project_dir}")
    
    # Clean up profiles directory
    profiles_dir = Path(DBT_PROJECT_CONFIG['profiles_dir'])
    if profiles_dir.exists():
        shutil.rmtree(profiles_dir)
        print(f"Cleaned up profiles directory: {profiles_dir}")

# Create the DAG
dag = DAG(
    'dbt_pipeline',
    default_args=DEFAULT_ARGS,
    description='Run dbt pipeline with credential management and artifact extraction',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'data', 'transform'],
)

# Task to run the main dbt pipeline
run_dbt_task = PythonOperator(
    task_id='run_dbt_pipeline',
    python_callable=dbt_pipeline,
    dag=dag,
)

# Optional: Upload artifacts to GCS (for GCP Composer)
upload_artifacts_task = PythonOperator(
    task_id='upload_artifacts_to_gcs',
    python_callable=lambda **context: print("Artifacts upload placeholder - implement based on your storage needs"),
    dag=dag,
)

# Task to clean up temporary files
cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=dag,
    trigger_rule='all_done',  # Run even if upstream tasks fail
)

# Define task dependencies
run_dbt_task >> upload_artifacts_task >> cleanup_task