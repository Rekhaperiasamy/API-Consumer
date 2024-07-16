import pytest
import httpx
from unittest.mock import patch, mock_open

from app.cluster_client import ClusterClient


@pytest.fixture
def cluster_client():
    return ClusterClient(simulate=False, hosts=['host1', 'host2', 'host3'])


def test_create_group_success(cluster_client):
    cluster_client.simulate = False
    with patch.object(cluster_client, '_make_post_request', return_value=True):
        assert cluster_client.create_group('test_group') == True


def test_create_group_failure(cluster_client):
    with patch.object(cluster_client, '_make_post_request', side_effect=[True, False, True]):
        with patch.object(cluster_client, '_rollback') as mock_rollback:
            assert cluster_client.create_group('test_group') == False
            mock_rollback.assert_called_once_with(
                'test_group', 'delete', ['host1'])


def test_delete_group_success(cluster_client):
    with patch.object(cluster_client, '_make_delete_request', return_value=True):
        assert cluster_client.delete_group('test_group') == True


def test_delete_group_failure(cluster_client):
    with patch.object(cluster_client, '_make_delete_request', side_effect=[True, False, True]):
        with patch.object(cluster_client, '_rollback') as mock_rollback:
            assert cluster_client.delete_group('test_group') == False
            mock_rollback.assert_called_once_with(
                'test_group', 'create', ['host1'])


def test_get_group_status(cluster_client):
    with patch.object(cluster_client, '_make_get_request', side_effect=[True, False, True]):
        status = cluster_client.get_group_status('test_group')
        assert status == {'host1': True, 'host2': False, 'host3': True}


def test_continue_rollbacks_success(cluster_client):
    mock_file_content = "delete\ntest_group\nhost1\nhost2"
    with patch('builtins.open', mock_open(read_data=mock_file_content)):
        with patch('os.path.exists', return_value=True):
            with patch('os.remove') as mock_remove:
                with patch.object(cluster_client, '_rollback', return_value=True) as mock_rollback:
                    assert cluster_client.continue_rollbacks() == True
                    mock_rollback.assert_called_once_with(
                        'test_group', 'delete', ['host1', 'host2'])
                    mock_remove.assert_called_once()


def test_continue_rollbacks_file_not_found(cluster_client):
    with patch('os.path.exists', return_value=False):
        try:
            cluster_client.continue_rollbacks()
            assert True
        except Exception as e:
            pytest.fail(f"Unexpected exception raised: {e}")


def test_continue_rollbacks_io_error(cluster_client):
    with patch('os.path.exists', return_value=True):
        with patch('builtins.open', side_effect=IOError):
            assert cluster_client.continue_rollbacks() == False


def test_rollback_success(cluster_client):
    with patch.object(cluster_client, '_make_delete_request', return_value=True):
        assert cluster_client._rollback('test_group', 'delete', [
                                        'host1', 'host2']) == True


def test_rollback_failure(cluster_client):
    with patch.object(cluster_client, '_make_delete_request', side_effect=[True, False]):
        with patch('builtins.open', mock_open()) as mock_file:
            assert cluster_client._rollback('test_group', 'delete', [
                                            'host1', 'host2']) == False
            mock_file().write.assert_any_call('delete\n')
            mock_file().write.assert_any_call('test_group\n')
            mock_file().write.assert_any_call('host2\n')


@patch('time.sleep', return_value=None)
def test_make_post_request_success(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_response = mock_client.return_value.__enter__.return_value.post.return_value
        mock_response.raise_for_status.return_value = None
        assert cluster_client._make_post_request('host1', 'test_group') == True


@patch('time.sleep', return_value=None)
def test_make_post_request_group_exists(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_response = mock_client.return_value.__enter__.return_value.post.return_value
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "400 error", request=None, response=httpx.Response(400)
        )
        assert cluster_client._make_post_request('host1', 'test_group') == True


@patch('time.sleep', return_value=None)
def test_make_post_request_failure(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_client.return_value.__enter__.return_value.post.side_effect = httpx.RequestError(
            "Connection error")
        assert cluster_client._make_post_request(
            'host1', 'test_group') == False


@patch('time.sleep', return_value=None)
def test_make_delete_request_success(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_response = mock_client.return_value.__enter__.return_value.delete.return_value
        mock_response.raise_for_status.return_value = None
        assert cluster_client._make_delete_request(
            'host1', 'test_group') == True


@patch('time.sleep', return_value=None)
def test_make_delete_request_failure(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_client.return_value.__enter__.return_value.delete.side_effect = Exception(
            "Delete error")
        assert cluster_client._make_delete_request(
            'host1', 'test_group') == False


@patch('time.sleep', return_value=None)
def test_make_get_request_success(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_response = mock_client.return_value.__enter__.return_value.get.return_value
        mock_response.raise_for_status.return_value = None
        assert cluster_client._make_get_request('host1', 'test_group') == True


@patch('time.sleep', return_value=None)
def test_make_get_request_not_found(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_response = mock_client.return_value.__enter__.return_value.get.return_value
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 error", request=None, response=httpx.Response(404)
        )
        assert cluster_client._make_get_request('host1', 'test_group') == False


@patch('time.sleep', return_value=None)
def test_make_get_request_failure(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_client.return_value.__enter__.return_value.get.side_effect = httpx.RequestError(
            "Connection error")
        assert cluster_client._make_get_request('host1', 'test_group') == False


@patch('time.sleep', return_value=None)
def test_make_post_request_retry_mechanism(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_post = mock_client.return_value.__enter__.return_value.post

        request = httpx.Request('DELETE', 'http://testserver')
        response = httpx.Response(status_code=200, request=request)
        mock_post.side_effect = [
            httpx.RequestError("Connection error"),
            httpx.RequestError("Connection error"),
            response
        ]

        assert cluster_client._make_post_request('host1', 'test_group') == True

        patched_time_sleep.call_count == 2
        assert mock_post.call_count == 3


@patch('time.sleep', return_value=None)
def test_make_post_request_retry_mechanism_failure(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_post = mock_client.return_value.__enter__.return_value.post
        mock_post.side_effect = httpx.RequestError("Connection error")

        assert cluster_client._make_post_request(
            'host1', 'test_group') == False

        assert patched_time_sleep.call_count == 3
        assert mock_post.call_count == 3


@patch('time.sleep', return_value=None)
def test_make_delete_request_retry_mechanism(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_delete = mock_client.return_value.__enter__.return_value.delete

        request = httpx.Request('DELETE', 'http://testserver')
        response = httpx.Response(status_code=200, request=request)
        mock_delete.side_effect = [
            Exception("Delete error"),
            Exception("Delete error"),
            response
        ]

        assert cluster_client._make_delete_request(
            'host1', 'test_group') == True

        assert patched_time_sleep.call_count == 2
        assert mock_delete.call_count == 3


@patch('time.sleep', return_value=None)
def test_make_delete_request_retry_mechanism_failure(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_delete = mock_client.return_value.__enter__.return_value.delete
        mock_delete.side_effect = Exception("Delete error")

        assert cluster_client._make_delete_request(
            'host1', 'test_group') == False

        assert patched_time_sleep.call_count == 3
        assert mock_delete.call_count == 3


@patch('time.sleep', return_value=None)
def test_make_get_request_retry_mechanism(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_get = mock_client.return_value.__enter__.return_value.get

        request = httpx.Request('GET', 'http://testserver')
        response = httpx.Response(status_code=200, request=request)
        mock_get.side_effect = [
            httpx.RequestError("Connection error"),
            httpx.RequestError("Connection error"),
            response
        ]

        assert cluster_client._make_get_request('host1', 'test_group') == True

        assert patched_time_sleep.call_count == 2
        assert mock_get.call_count == 3


@patch('time.sleep', return_value=None)
def test_make_get_request_retry_mechanism_failure(patched_time_sleep, cluster_client):
    with patch('httpx.Client') as mock_client:
        mock_get = mock_client.return_value.__enter__.return_value.get
        mock_get.side_effect = httpx.RequestError("Connection error")

        assert cluster_client._make_get_request('host1', 'test_group') == False

        assert patched_time_sleep.call_count == 3
        assert mock_get.call_count == 3
