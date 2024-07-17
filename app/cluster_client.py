import httpx
import random
import time
import os
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ClusterClient:
    def __init__(self, hosts: List[str], simulate: bool = False, retry_timeout: int = 1, max_retries: int = 3, rollback_file: str = 'rollback.txt'):
        self.hosts = hosts
        self.base_url = "http://{}/v1/group/"
        self.retry_timeout = retry_timeout
        self.simulate = simulate
        self.max_retries = max_retries
        self.rollback_file = rollback_file
        self.request_timeout = 1

    def create_group(self, group_id: str) -> bool:
        successful_hosts = []
        for host in self.hosts:
            if self._make_post_request(host=host, group_id=group_id):
                successful_hosts.append(host)
            else:
                logger.info(
                    f"Create group failed on host {host}. Attempting rollback...")
                self._rollback(group_id, 'delete', successful_hosts)
                return False

        return True

    def delete_group(self, group_id: str) -> bool:
        successful_hosts = []
        for host in self.hosts:
            success = self._make_delete_request(
                host=host, group_id=group_id)
            if success:
                successful_hosts.append(host)
            else:
                logger.info(
                    f"Delete group failed on host {host}. Attempting rollback...")
                self._rollback(group_id, 'create', successful_hosts)
                return False
        return True

    def get_group_status(self, group_id: str) -> Dict[str, bool]:
        status = {}
        for host in self.hosts:
            success = self._make_get_request(host, group_id)
            status[host] = success
        return status

    def continue_rollbacks(self) -> bool:
        if os.path.exists(self.rollback_file):
            try:
                with open(self.rollback_file, 'r') as file:
                    operation = file.readline().strip()
                    group_id = file.readline().strip()
                    hosts = [line.strip() for line in file]
            except IOError as e:
                logger.error(f"Error reading rollback file: {e}")
                return False
            os.remove(self.rollback_file)
            return self._rollback(group_id, operation, hosts)

    def _rollback(self, group_id: str, operation: str, hosts_to_rollback: List[str]) -> bool:
        failed_rollbacks = []
        for host in hosts_to_rollback:
            if operation == 'delete':
                if not self._make_delete_request(host=host, group_id=group_id):
                    failed_rollbacks.append(host)
                    logger.info(
                        f"Rollback failed for host {host} during {operation} operation")
            elif operation == 'create':
                if not self._make_post_request(host=host, group_id=group_id):
                    failed_rollbacks.append(host)
                    logger.info(
                        f"Rollback failed for host {host} during {operation} operation")

        if not failed_rollbacks:
            logger.info(
                f"Rollback operation successful")
            return True
        else:
            logger.info(
                "Found failed rollback operations. storing it in a file to continue later...")
            with open(self.rollback_file, 'w') as file:
                file.write(operation + '\n')
                file.write(group_id + '\n')
                for host in failed_rollbacks:
                    file.write(host + '\n')

            return False

    def _make_post_request(self, host: str, group_id: str) -> bool:
        if self.simulate:
            return random.choice([True, False])

        url = self.base_url.format(host)
        data = {"groupId": group_id}
        backoff = self.retry_timeout

        for attempt in range(self.max_retries):
            try:
                with httpx.Client(timeout=self.request_timeout) as client:
                    response = client.post(
                        url, json=data, timeout=self.request_timeout)

                response.raise_for_status()
                return True
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.info(
                        f"Group already exist on host {host}. Moving on to next host...")
                    return True
            except httpx.RequestError as e:
                logger.info(
                    f"create attempt {attempt + 1} failed for host {host}. Retrying...")
            time.sleep(backoff)
            backoff *= 2
        return False

    def _make_delete_request(self, host: str, group_id: str) -> bool:
        if self.simulate:
            return random.choice([True, False])

        url = self.base_url.format(host)
        data = {"groupId": group_id}
        backoff = self.retry_timeout

        for attempt in range(self.max_retries):
            try:
                with httpx.Client(timeout=self.request_timeout) as client:
                    response = client.delete(
                        url, json=data, timeout=self.request_timeout)

                response.raise_for_status()
                return True
            except Exception as e:
                logger.info(
                    f"delete attempt {attempt + 1} failed for host {host}. Retrying...")
            time.sleep(backoff)
            backoff *= 2
        return False

    def _make_get_request(self, host: str, group_id: str) -> bool:
        if self.simulate:
            return random.choice([True, False])

        url = f"{self.base_url.format(host)}/{group_id}"
        backoff = self.retry_timeout

        for attempt in range(self.max_retries):
            try:
                with httpx.Client(timeout=self.request_timeout) as client:
                    response = client.get(url, timeout=self.request_timeout)
                response.raise_for_status()
                return True
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.info(
                        f"Get request failed for host {host}. Status code: {e.response.status_code}")
                    return False
            except httpx.RequestError as e:
                logger.info(
                    f"Get attempt {attempt + 1} failed for host {host}. Retrying...")
            time.sleep(backoff)
            backoff *= 2
        return False
