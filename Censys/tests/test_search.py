import json
import os

import pytest

from censys_module.search import SearchAction


@pytest.fixture
def result():
    return {
        "status": "ok",
        "results": [
            {"ip": "83.84.160.31", "updated_at": "2019-10-23T05:46:40+00:00"},
            {"ip": "45.61.253.130", "updated_at": "2019-10-23T05:47:15+00:00"},
            {"ip": "82.199.62.27", "updated_at": "2019-10-23T05:47:25+00:00"},
            {"ip": "80.211.60.119", "updated_at": "2019-10-23T05:54:51+00:00"},
            {"ip": "154.81.248.132", "updated_at": "2019-10-23T05:53:48+00:00"},
            {"ip": "65.191.36.219", "updated_at": "2019-10-23T05:52:36+00:00"},
            {"ip": "86.30.126.184", "updated_at": "2019-10-23T05:57:35+00:00"},
            {"ip": "108.187.129.129", "updated_at": "2019-10-23T05:32:05+00:00"},
            {"ip": "85.31.46.31", "updated_at": "2019-10-23T05:32:10+00:00"},
            {"ip": "88.162.154.96", "updated_at": "2019-10-23T05:32:17+00:00"},
            {"ip": "104.149.242.31", "updated_at": "2019-10-23T05:31:20+00:00"},
            {"ip": "154.194.84.27", "updated_at": "2019-10-23T05:28:04+00:00"},
            {"ip": "141.212.121.180", "updated_at": "2019-10-23T05:28:07+00:00"},
            {"ip": "58.248.183.99", "updated_at": "2019-10-23T05:28:44+00:00"},
            {"ip": "156.236.42.78", "updated_at": "2019-10-23T05:31:37+00:00"},
            {"ip": "107.175.45.81", "updated_at": "2019-10-23T05:28:16+00:00"},
            {"ip": "216.234.166.137", "updated_at": "2019-10-23T05:28:32+00:00"},
            {"ip": "107.167.40.42", "updated_at": "2019-10-23T05:25:37+00:00"},
            {"ip": "172.82.154.115", "updated_at": "2019-10-23T05:23:08+00:00"},
            {"ip": "54.169.39.237", "updated_at": "2019-10-23T05:24:52+00:00"},
            {"ip": "23.235.141.60", "updated_at": "2019-10-23T05:26:01+00:00"},
            {"ip": "18.234.253.153", "updated_at": "2019-10-23T05:26:12+00:00"},
            {"ip": "23.94.242.185", "updated_at": "2019-10-23T05:56:08+00:00"},
            {"ip": "64.141.130.244", "updated_at": "2019-10-23T05:51:57+00:00"},
            {"ip": "31.184.253.44", "updated_at": "2019-10-23T05:53:14+00:00"},
            {"ip": "160.124.217.8", "updated_at": "2019-10-23T05:53:15+00:00"},
            {"ip": "52.57.104.191", "updated_at": "2019-10-23T06:08:02+00:00"},
            {"ip": "154.214.211.110", "updated_at": "2019-10-23T06:08:03+00:00"},
            {"ip": "180.44.81.106", "updated_at": "2019-10-23T06:08:06+00:00"},
            {"ip": "185.171.158.7", "updated_at": "2019-10-23T06:08:20+00:00"},
            {"ip": "52.221.253.108", "updated_at": "2019-10-23T06:08:23+00:00"},
            {"ip": "104.161.97.112", "updated_at": "2019-10-23T06:08:33+00:00"},
            {"ip": "154.208.9.33", "updated_at": "2019-10-23T06:06:41+00:00"},
            {"ip": "102.164.51.87", "updated_at": "2019-10-23T06:07:31+00:00"},
            {"ip": "107.174.182.114", "updated_at": "2019-10-23T06:06:22+00:00"},
            {"ip": "34.244.195.33", "updated_at": "2019-10-23T06:08:18+00:00"},
            {"ip": "52.198.75.192", "updated_at": "2019-10-23T06:06:20+00:00"},
            {"ip": "82.223.77.30", "updated_at": "2019-10-23T06:06:29+00:00"},
            {"ip": "156.241.130.80", "updated_at": "2019-10-23T06:06:29+00:00"},
            {"ip": "104.149.104.57", "updated_at": "2019-10-23T06:01:06+00:00"},
            {"ip": "154.89.228.132", "updated_at": "2019-10-23T06:01:12+00:00"},
            {"ip": "96.62.10.170", "updated_at": "2019-10-23T06:02:38+00:00"},
            {"ip": "117.159.24.73", "updated_at": "2019-10-23T06:01:58+00:00"},
            {"ip": "216.194.85.92", "updated_at": "2019-10-23T06:01:57+00:00"},
            {"ip": "52.184.73.47", "updated_at": "2019-10-23T06:02:58+00:00"},
            {"ip": "107.163.211.93", "updated_at": "2019-10-23T06:00:06+00:00"},
            {"ip": "160.122.211.154", "updated_at": "2019-10-23T06:00:11+00:00"},
            {"ip": "172.255.79.254", "updated_at": "2019-10-23T05:58:09+00:00"},
            {"ip": "149.62.47.19", "updated_at": "2019-10-23T05:54:10+00:00"},
            {"ip": "185.81.210.191", "updated_at": "2019-10-23T06:09:21+00:00"},
            {"ip": "2.203.250.157", "updated_at": "2019-10-23T06:08:48+00:00"},
            {"ip": "88.167.125.121", "updated_at": "2019-10-23T06:09:54+00:00"},
            {"ip": "54.95.128.252", "updated_at": "2019-10-23T06:10:00+00:00"},
            {"ip": "78.209.239.104", "updated_at": "2019-10-23T06:09:55+00:00"},
            {"ip": "82.245.142.223", "updated_at": "2019-10-23T06:10:51+00:00"},
            {"ip": "172.107.186.246", "updated_at": "2019-10-23T06:30:28+00:00"},
            {"ip": "193.148.70.30", "updated_at": "2019-10-23T06:50:12+00:00"},
            {"ip": "94.21.223.140", "updated_at": "2019-10-23T06:49:36+00:00"},
            {"ip": "51.255.221.251", "updated_at": "2019-10-23T06:48:33+00:00"},
            {"ip": "78.234.219.14", "updated_at": "2019-10-23T06:48:46+00:00"},
            {"ip": "45.34.114.53", "updated_at": "2019-10-23T06:47:31+00:00"},
            {"ip": "182.87.199.24", "updated_at": "2019-10-23T06:47:53+00:00"},
            {"ip": "147.255.87.201", "updated_at": "2019-10-23T06:46:54+00:00"},
            {"ip": "204.15.74.54", "updated_at": "2019-10-23T06:26:26+00:00"},
            {"ip": "198.15.167.14", "updated_at": "2019-10-23T06:03:25+00:00"},
            {"ip": "156.247.74.146", "updated_at": "2019-10-23T06:03:28+00:00"},
            {"ip": "3.8.165.165", "updated_at": "2019-10-23T06:23:09+00:00"},
            {"ip": "173.82.254.85", "updated_at": "2019-10-23T06:23:11+00:00"},
            {"ip": "198.15.230.245", "updated_at": "2019-10-23T06:26:47+00:00"},
            {"ip": "129.204.48.140", "updated_at": "2019-10-23T06:26:50+00:00"},
            {"ip": "94.210.134.29", "updated_at": "2019-10-23T06:23:28+00:00"},
            {"ip": "198.105.244.28", "updated_at": "2019-10-23T06:23:38+00:00"},
            {"ip": "167.88.162.133", "updated_at": "2019-10-23T06:27:15+00:00"},
            {"ip": "142.234.117.123", "updated_at": "2019-10-23T06:42:38+00:00"},
            {"ip": "123.149.43.64", "updated_at": "2019-10-23T06:42:45+00:00"},
            {"ip": "88.164.7.95", "updated_at": "2019-10-23T06:46:33+00:00"},
            {"ip": "46.244.241.246", "updated_at": "2019-10-23T06:43:10+00:00"},
            {"ip": "107.158.76.158", "updated_at": "2019-10-23T06:43:42+00:00"},
            {"ip": "185.182.8.137", "updated_at": "2019-10-23T06:37:52+00:00"},
            {"ip": "160.122.135.178", "updated_at": "2019-10-23T06:37:58+00:00"},
            {"ip": "172.74.203.206", "updated_at": "2019-10-23T06:40:55+00:00"},
            {"ip": "172.241.175.179", "updated_at": "2019-10-23T06:39:39+00:00"},
            {"ip": "172.241.10.147", "updated_at": "2019-10-23T06:42:28+00:00"},
            {"ip": "78.213.35.178", "updated_at": "2019-10-23T06:57:01+00:00"},
            {"ip": "82.255.220.176", "updated_at": "2019-10-23T06:53:09+00:00"},
            {"ip": "162.209.129.172", "updated_at": "2019-10-23T06:55:50+00:00"},
            {"ip": "156.241.206.202", "updated_at": "2019-10-23T06:56:14+00:00"},
            {"ip": "172.241.159.229", "updated_at": "2019-10-23T06:56:53+00:00"},
            {"ip": "192.74.248.200", "updated_at": "2019-10-23T06:56:57+00:00"},
            {"ip": "154.214.6.148", "updated_at": "2019-10-23T06:57:14+00:00"},
            {"ip": "35.243.116.152", "updated_at": "2019-10-23T06:57:14+00:00"},
            {"ip": "156.227.112.211", "updated_at": "2019-10-23T06:58:46+00:00"},
            {"ip": "156.247.92.22", "updated_at": "2019-10-23T06:59:22+00:00"},
            {"ip": "142.234.114.83", "updated_at": "2019-10-23T06:58:52+00:00"},
            {"ip": "52.77.229.238", "updated_at": "2019-10-23T06:57:28+00:00"},
            {"ip": "198.105.185.245", "updated_at": "2019-10-23T06:59:58+00:00"},
            {"ip": "154.202.209.27", "updated_at": "2019-10-23T07:00:04+00:00"},
            {"ip": "23.107.118.55", "updated_at": "2019-10-23T07:00:08+00:00"},
            {"ip": "88.153.246.3", "updated_at": "2019-10-23T06:59:02+00:00"},
            {"ip": "154.213.57.209", "updated_at": "2019-10-23T06:49:23+00:00"},
        ],
        "metadata": {
            "count": 5_085_359,
            "query": "80.http.get.headers.server: nginx",
            "backend_time": 245,
            "page": 1,
            "pages": 50854,
        },
    }


@pytest.fixture
def action():
    action = SearchAction()
    action.module.configuration = {"api_user_id": "foo", "api_user_secret": "bar"}
    yield action


def mock_request(censys_mock, arguments, json):
    censys_mock.post(f'https://www.censys.io/api/v1/search/{arguments["index"]}', json=json)


def validate_result(res, expected, storage):
    assert "result_path" in res
    file_path = os.path.join(storage, res["result_path"])
    assert os.path.isfile(file_path)
    with open(file_path) as fp:
        loaded = json.load(fp)
    assert loaded == expected


def test_search(action, symphony_storage, result, censys_mock):
    arguments = {
        "index": "ipv4",
        "item": "8.8.8.8",
        "query": "80.http.get.headers.server: nginx",
    }
    mock_request(censys_mock, arguments, result)
    res = action.run(arguments)
    validate_result(res, result["results"], symphony_storage)


def test_search_2_requests(action, symphony_storage, result, censys_mock):
    arguments = {
        "index": "ipv4",
        "item": "8.8.8.8",
        "query": "80.http.get.headers.server: nginx",
        "max_requests": 2,
    }

    mock_request(censys_mock, arguments, result)
    res = action.run(arguments)
    expected = result["results"] + result["results"]
    validate_result(res, expected, symphony_storage)


def test_search_last_run(action):
    arguments = {
        "index": "ipv4",
        "item": "8.8.8.8",
        "query": "80.http.get.headers.server: nginx",
        "max_requests": 2,
        "last_run": "2019-10-12T10:22:45",
    }

    assert action.get_query(arguments) == f"({arguments['query']}) AND updated_at: [{arguments['last_run']} TO *]"
