def all_users_set() -> str:
    """
    Generate the Redis key for the set containing all users.

    Returns:
        str: The Redis key for the all users set.
    """
    return 'allUsers'


def user_details_htable(email: str) -> str:
    """
    Generate the Redis key for a user's details hash table.

    Args:
        email (str): The email of the user.

    Returns:
        str: The Redis key for the user's details hash table.
    """
    return f'user:{email}'


def user_active_api_keys_set(email: str) -> str:
    """
    Generate the Redis key for a user's active API keys set.

    Args:
        email (str): The email of the user.

    Returns:
        str: The Redis key for the user's active API keys set.
    """
    return f'user:{email}:apikeys'


def user_revoked_api_keys_set(email: str) -> str:
    """
    Generate the Redis key for a user's revoked API keys set.

    Args:
        email (str): The email of the user.

    Returns:
        str: The Redis key for the user's revoked API keys set.
    """
    return f'user:{email}:revokedApikeys'


def api_key_to_owner_key(api_key: str) -> str:
    """
    Generate the Redis key that maps an API key to its owner.

    Args:
        api_key (str): The API key.

    Returns:
        str: The Redis key that maps the API key to its owner.
    """
    return f'apikey:{api_key}:owner'
