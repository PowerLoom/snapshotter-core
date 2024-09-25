import json

import psutil
import redis

from snapshotter.settings.config import settings
from snapshotter.utils.redis.redis_conn import REDIS_CONN_CONF


def process_up(pid):
    """
    Check if a process with given PID is running or not.

    Args:
        pid (int): Process ID to check.

    Returns:
        bool: True if process is running, False otherwise.
    """
    p_ = psutil.Process(pid)
    return p_.is_running()
    # Note: The following commented-out code blocks are alternative implementations
    # that were likely used for testing or debugging purposes.
    # They are kept here for reference but are not currently in use.

    # Alternative implementation using os.waitpid
    # try:
    #     return os.waitpid(pid, os.WNOHANG) is not None
    # except ChildProcessError:  # no child processes
    #     return False

    # Alternative implementation using subprocess
    # try:
    #     call = subprocess.check_output("pidof '{}'".format(self.processName), shell=True)
    #     return True
    # except subprocess.CalledProcessError:
    #     return False


def main():
    """
    Retrieve process details from Redis cache and print their running status.

    This function performs the following steps:
    1. Connects to Redis using the configured connection details.
    2. Retrieves process information for the System Event Detector, Worker Processor Distributor, and Worker Processes.
    3. Checks the running status of each process using the `process_up` function.
    4. Prints the status information for each process.

    The function handles potential errors such as invalid PIDs or corrupted data in the Redis cache.
    """
    # Establish Redis connection
    connection_pool = redis.BlockingConnectionPool(**REDIS_CONN_CONF)
    redis_conn = redis.Redis(connection_pool=connection_pool)

    # Retrieve process map from Redis
    map_raw = redis_conn.hgetall(
        name=f'powerloom:snapshotter:{settings.namespace}:{settings.instance_id}:Processes',
    )

    # Check System Event Detector status
    print('\n' + '=' * 20 + 'System Event Detector' + '=' * 20)
    event_det_pid = map_raw[b'SystemEventDetector']
    try:
        event_det_pid = int(event_det_pid)
        print('Event detector process running status: ', process_up(event_det_pid))
    except ValueError:
        print('Event detector pid found in process map not a PID: ', event_det_pid)

    # Check Worker Processor Distributor status
    print('\n' + '=' * 20 + 'Worker Processor Distributor' + '=' * 20)
    proc_dist_pid = map_raw[b'ProcessorDistributor']
    try:
        proc_dist_pid = int(proc_dist_pid)
        print('Processor distributor process running status: ', process_up(proc_dist_pid))
    except ValueError:
        print('Processor distributor pid found in process map not a PID: ', proc_dist_pid)

    # Check Worker Processes status
    print('\n' + '=' * 20 + 'Worker Processes' + '=' * 20)
    cb_worker_map = map_raw[b'callback_workers']
    try:
        cb_worker_map = json.loads(cb_worker_map)
    except json.JSONDecodeError:
        print('Callback worker entries in cache corrupted...', cb_worker_map)
        return

    for worker_type, worker_details in cb_worker_map.items():
        section_name = worker_type.capitalize()
        print('\n' + '*' * 10 + section_name + '*' * 10)
        if not worker_details or not isinstance(worker_details, dict):
            print(f'No {section_name} workers found in process map: ', worker_details)
            continue
        for short_id, worker_details in worker_details.items():
            print('\n' + '-' * 5 + short_id + '-' * 5)
            proc_pid = worker_details['pid']
            try:
                proc_pid = int(proc_pid)
                print('Process name ' + worker_details['id'] + ' running status: ', process_up(proc_pid))
            except ValueError:
                print(f'Process name {worker_details["id"]} pid found in process map not a PID: ', proc_pid)


if __name__ == '__main__':
    main()
