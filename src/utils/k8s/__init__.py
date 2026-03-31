from functools import cache

from kubernetes import client, config


@cache
def get_apps_v1_api():
    config.load_config()
    return client.AppsV1Api()


@cache
def get_core_v1_api():
    config.load_config()
    return client.CoreV1Api()


@cache
def get_custom_objects_api():
    config.load_config()
    api = client.CustomObjectsApi()
    api.api_client.set_default_header("Content-Type", "application/json")
    return api
