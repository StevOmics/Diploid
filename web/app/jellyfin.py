import time

import httpx

_users_cache: dict[str, tuple[float, list[dict]]] = {}
USERS_CACHE_TTL_SECONDS = 300


def test_connection(server_url: str, api_key: str) -> dict:
    url = server_url.rstrip("/") + "/System/Info"
    try:
        response = httpx.get(url, headers={"X-Emby-Token": api_key}, timeout=5.0)
    except httpx.RequestError as exc:
        return {"ok": False, "message": f"Could not reach server: {exc}"}

    if response.status_code == 401:
        return {"ok": False, "message": "Rejected: invalid API key"}
    if response.status_code != 200:
        return {"ok": False, "message": f"Server responded with {response.status_code}"}

    data = response.json()
    server_name = data.get("ServerName", "Jellyfin")
    version = data.get("Version", "unknown")
    return {"ok": True, "message": f"Connected to {server_name} (version {version})"}


def list_users(server_url: str, api_key: str) -> list[dict]:
    url = server_url.rstrip("/") + "/Users"
    response = httpx.get(url, headers={"X-Emby-Token": api_key}, timeout=5.0)
    response.raise_for_status()
    return [{"id": u["Id"], "name": u["Name"]} for u in response.json()]


def list_users_cached(server_url: str, api_key: str) -> list[dict]:
    """Same as list_users, but reuses a result younger than USERS_CACHE_TTL_SECONDS.

    The Settings page calls this on every load - Jellyfin's /Users endpoint is
    a real network round trip (slow, and unnecessary since the user list rarely
    changes), so without caching every Settings page view pays that cost.
    """
    key = f"{server_url}|{api_key}"
    now = time.monotonic()
    cached = _users_cache.get(key)
    if cached and now - cached[0] < USERS_CACHE_TTL_SECONDS:
        return cached[1]

    users = list_users(server_url, api_key)
    _users_cache[key] = (now, users)
    return users


def list_libraries(server_url: str, api_key: str) -> list[dict]:
    url = server_url.rstrip("/") + "/Library/VirtualFolders"
    response = httpx.get(url, headers={"X-Emby-Token": api_key}, timeout=5.0)
    response.raise_for_status()
    return [
        {"name": f["Name"], "locations": f.get("Locations", [])}
        for f in response.json()
    ]


def fetch_movie_watch_data(server_url: str, api_key: str, user_id: str) -> list[dict]:
    """Fetch every movie item + this user's watch data, handling pagination."""
    url = server_url.rstrip("/") + f"/Users/{user_id}/Items"
    headers = {"X-Emby-Token": api_key}
    items: list[dict] = []
    start_index = 0
    page_size = 200

    while True:
        response = httpx.get(
            url,
            headers=headers,
            timeout=15.0,
            params={
                "Recursive": "true",
                "IncludeItemTypes": "Movie",
                "Fields": "Path,UserData",
                "StartIndex": start_index,
                "Limit": page_size,
            },
        )
        response.raise_for_status()
        payload = response.json()
        page_items = payload.get("Items", [])
        items.extend(page_items)

        start_index += len(page_items)
        if len(page_items) < page_size or start_index >= payload.get("TotalRecordCount", 0):
            break

    return items
