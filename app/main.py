import argparse
import os
import logging
from pathlib import Path
from app.cluster_client import ClusterClient

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

hosts_file_path = Path(__file__).parent / 'hosts.txt'
rollback_file_path = Path(__file__).parent / 'rollback.txt'


def read_hosts_file(file_path):
    try:
        with open(file_path, 'r') as file:
            hosts = [line.strip() for line in file if line.strip()]
        if not hosts:
            logger.error(f"No hosts found in {file_path}")
            return None
        return hosts
    except FileNotFoundError:
        logger.error(f"Hosts file not found: {file_path}")
        return None
    except IOError as e:
        logger.error(f"Error reading hosts file: {e}")
        return None


def rollback(client):
    if os.path.exists(rollback_file_path):
        logger.info("Found existing rollback.txt. Processing rollback")
        if not client.continue_rollbacks():
            logger.error("Rollbacks failed again. Try again!")
            return False
        else:
            return True
    else:
        logger.info("No pending rollbacks found")
        return True


def perform_operation(client, operation, group_name):
    if operation == 'create':
        logger.info(f"Attempting to create group: {group_name}")
        rollback_result = rollback(client)
        if not rollback_result:
            return False
        success = client.create_group(group_name)
        if success:
            logger.info(f"Group {group_name} created successfully")
            return True
        else:
            logger.error(
                f"Failed to create group {group_name} after all retries")
            return False
    elif operation == 'delete':
        logger.info(f"Attempting to delete group: {group_name}")
        rollback_result = rollback(client)
        if not rollback_result:
            return False
        success = client.delete_group(group_name)
        if success:
            logger.info(f"Group {group_name} deleted successfully")
            return True
        else:
            logger.error(
                f"Failed to delete group {group_name} after all retries")
            return False
    elif operation == 'status':
        logger.info(f"Checking status of group: {group_name}")
        status = client.get_group_status(group_name)
        if status:
            logger.info(f"Group {group_name} status:")
            for host, exists in status.items():
                logger.info(
                    f"{host}: {'Exists' if exists else 'Does not exist'}")
            return True
        else:
            logger.error(f"Failed to get status for group {group_name}")
            return False

    elif operation == 'rollback':
        logger.info("Performing rollback operation")
        success = rollback(client)
        if success:
            logger.info("Rollback completed successfully")
            return True
        else:
            logger.error("Rollback failed")
            return False


def main():
    parser = argparse.ArgumentParser(description="Manage cluster groups.")
    parser.add_argument('--operation', type=str, choices=[
                        'create', 'delete', 'status', 'rollback'], required=True, help="The operation to perform on the group: create, delete, status or rollback")
    parser.add_argument('--group_name', type=str, required=True,
                        help="The name of the group to create or delete")
    parser.add_argument('--simulate', type=bool, default=True,
                        help="Simulate the operations (default: True)")
    parser.add_argument('--max_retries', type=int, default=2,
                        help="Maximum number of retries (default: 2)")
    parser.add_argument('--retry_timeout', type=int, default=1,
                        help="Retry timeout in seconds (default: 1)")
    args = parser.parse_args()

    group_name = args.group_name
    operation = args.operation
    simulate = args.simulate
    max_retries = args.max_retries
    retry_timeout = args.retry_timeout

    hosts = read_hosts_file(hosts_file_path)
    if not hosts:
        logger.error("No hosts found in hosts.txt")
        return

    client = ClusterClient(hosts, simulate=simulate,
                           max_retries=max_retries, retry_timeout=retry_timeout, rollback_file=rollback_file_path)

    perform_operation(client=client, operation=operation,
                      group_name=group_name)


if __name__ == "__main__":
    main()
