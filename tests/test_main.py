import pytest
import logging
from unittest.mock import patch, mock_open, Mock

from app.main import perform_operation, read_hosts_file, rollback

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_client():
    return Mock()


def test_read_hosts_file_success():
    with patch('builtins.open', new_callable=mock_open, read_data="host1\nhost2\nhost3"):
        result = read_hosts_file('dummy_path')
        assert result == ['host1', 'host2', 'host3']


def test_read_hosts_file_empty():
    with patch('builtins.open', new_callable=mock_open, read_data=""):
        result = read_hosts_file('dummy_path')
        assert result is None


def test_read_hosts_file_not_found():
    with patch('builtins.open', side_effect=FileNotFoundError):
        result = read_hosts_file('dummy_path')
        assert result is None


def test_read_hosts_file_io_error():
    with patch('builtins.open', side_effect=IOError):
        result = read_hosts_file('dummy_path')
        assert result is None


def test_rollback_file_exists_and_continue_rollbacks_succeeds():
    with patch('os.path.exists', return_value=True):
        client = Mock()
        client.continue_rollbacks.return_value = True
        result = rollback(client)
        assert result is True


def test_rollback_file_exists_and_continue_rollbacks_fails():
    with patch('os.path.exists', return_value=True):
        client = Mock()
        client.continue_rollbacks.return_value = False
        result = rollback(client)
        assert result is False


def test_rollback_file_does_not_exist():
    with patch('os.path.exists', return_value=False):
        client = Mock()
        result = rollback(client)
        assert result is True


@pytest.mark.parametrize("operation, group_name, client_method, client_return, expected_result", [
    ('create', 'test_group', 'create_group', True, True),
    ('create', 'test_group', 'create_group', False, False),
    ('delete', 'test_group', 'delete_group', True, True),
    ('delete', 'test_group', 'delete_group', False, False),
    ('status', 'test_group', 'get_group_status',
     {'host1': True, 'host2': False}, True),
    ('status', 'test_group', 'get_group_status', None, False),
])
def test_perform_operation(mock_client, operation, group_name, client_method, client_return, expected_result):
    setattr(mock_client, client_method, Mock(return_value=client_return))

    with patch.object(logger, 'info') as mock_info, patch.object(logger, 'error') as mock_error:
        result = perform_operation(mock_client, operation, group_name)

        assert result == expected_result
        getattr(mock_client, client_method).assert_called_once_with(group_name)


def test_perform_operation_invalid_operation(mock_client):
    result = perform_operation(mock_client, 'invalid_operation', 'test_group')
    assert result is None
