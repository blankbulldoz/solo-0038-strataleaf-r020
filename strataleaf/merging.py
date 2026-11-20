from .codec import dumps, loads


def merge_patch(document, patch):
    if not isinstance(patch, dict):
        return loads(dumps(patch))
    result = dict(document) if isinstance(document, dict) else {}
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        else:
            result[key] = merge_patch(result.get(key), value)
    return result
