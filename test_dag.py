from datetime import datetime
import platform
import socket
import os

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_environment():
    """Print pod/container environment info."""
    print(f"Hostname: {socket.gethostname()}")
    print(f"Python: {platform.python_version()}")
    print(f"Platform: {platform.platform()}")
    print(f"User: {os.getuid()}:{os.getgid()}")
    print(f"Working dir: {os.getcwd()}")
    print(f"CPU count: {os.cpu_count()}")


def do_computation():
    """Simple CPU work to verify executor actually runs tasks."""
    total = sum(i * i for i in range(1_000_000))
    print(f"Computation result: {total}")
    return total


def check_network():
    """Verify DNS and outbound connectivity from worker pod."""
    # DNS resolution
    api_ip = socket.getaddrinfo("airflow-api-server", 8080)[0][4][0]
    print(f"API server resolved: {api_ip}")

    # Check if we can reach the DB host
    try:
        db_ip = socket.getaddrinfo("ml-airflow.pyn.ru", 5432)[0][4][0]
        print(f"DB host resolved: {db_ip}")
    except socket.gaierror as e:
        print(f"DB host DNS failed: {e}")


with DAG(
    "test_infra",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:
    t1 = PythonOperator(task_id="check_env", python_callable=check_environment)
    t2 = PythonOperator(task_id="compute", python_callable=do_computation)
    t3 = PythonOperator(task_id="check_net", python_callable=check_network)

    t1 >> t2 >> t3
