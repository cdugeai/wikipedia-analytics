import json

def format_change(change_str: str) -> object:
    change = json.loads(change_str)
    return {
        "meta": {
            "uri": change["meta"]["uri"],
            "id": change["meta"]["id"],
            "domain": change["meta"]["domain"],
            "dt": change["meta"]["dt"],
            "offset": change["meta"]["offset"],
        },
        "id": change.get("id"),
        "title": change.get("title"),
        "comment": change.get("comment"),
        "timestamp": change.get("timestamp"),
        "user": change.get("user"),
        "bot": change.get("bot"),
        "length": change.get("length"),
        "server_url": change.get("server_url"),
        "server_name": change.get("server_name"),
        "wiki": change.get("wiki"),
        "parsedcomment": change.get("parsedcomment"),
    }
