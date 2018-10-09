def find_dict_in_list(l, exact_match=True, **kwargs):
    """In a list of similarly-keyed dictionaries, find the ones with the keys
    given.

    e.g.
    >>> l = [{"a":2, "b":5}, {"a":3, "b":5}]
    >>> find_dict_in_list(l, a=3)
    {"a":3, "b":5}
    >>> find_dict_in_list(l, b=7)
    None
    >>> find_dict_in_list(l, b=5)
    ValueError(...)
    >>> find_dict_in_list(l, b=5, exact_match=False)
    [{"a":2, "b":5}, {"a":3, "b":5}]
    >>> find_dict_in_list(l, b=7, exact_match=False)
    []
    """

    filtered_list = []

    for d in l:
        for k, v in kwargs.items():
            if d[k] != v:
                break
        else:
            filtered_list.append(d)

    if not exact_match:
        return filtered_list

    if len(filtered_list) == 1:
        return filtered_list[0]
    elif len(filtered_list) == 0:
        return None
    else:
        raise ValueError(
            "Too many matches!", dict(matches=filtered_list, predicates=kwargs)
        )
