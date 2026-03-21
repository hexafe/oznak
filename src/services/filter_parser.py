from src.query.builder import parse_filter_string


def parse_filters(filters: list = None, last: int = None):
    if not filters:
        filters = []

    for filter_str in filters:
        parse_filter_string(filter_str)

    return {
        "filters": filters,
        "limit": last
    }
