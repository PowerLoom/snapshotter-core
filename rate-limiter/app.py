import os
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from typing import Dict

from dotenv import load_dotenv
from fastapi import Depends
from fastapi import FastAPI
from fastapi import Request
from pydantic import BaseModel
from slowapi import _rate_limit_exceeded_handler
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

# Load environment variables from .env file
load_dotenv()

# Configuration
# Default rate limit if not specified in environment or for a specific key
RATE_LIMIT = os.getenv('DEFAULT_RATE_LIMIT', '10')

DEFAULT_RATE_LIMIT = f'{RATE_LIMIT}/second'

print(f'DEFAULT_RATE_LIMIT: {DEFAULT_RATE_LIMIT}')

# Initialize FastAPI app with title
app = FastAPI(title='Rate Limiter')

# In-memory statistics storage
# Format: {key: {'hourly': {timestamp: count}, 'daily': {timestamp: count}}}
# This stores usage statistics for each key, organized by hour and day
stats_storage = defaultdict(lambda: {'hourly': {}, 'daily': {}})

# In-memory rate limit storage
# Format: {key: rate_limit_string}
# This stores custom rate limits for each key
rate_limits = {}


def get_key_func(request: Request) -> str:
    """
    Extract the key from path parameters or use IP address as fallback.

    This function is used by the rate limiter to determine which key to use
    for rate limiting. It first checks if a key is provided in the path
    parameters, and if not, falls back to the client's IP address.

    Args:
        request (Request): The FastAPI request object

    Returns:
        str: The key to use for rate limiting
    """
    if hasattr(request, 'path_params') and 'key' in request.path_params:
        return request.path_params['key']
    return get_remote_address(request)


# Initialize rate limiter with our custom key function
limiter = Limiter(key_func=get_key_func)
app.state.limiter = limiter
# Register the rate limit exceeded handler to return appropriate HTTP responses
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# Pydantic models for request/response validation
class RateLimitConfig(BaseModel):
    """
    Model for configuring a rate limit for a specific key.

    Attributes:
        key (str): The key to configure the rate limit for
        limit (str): The rate limit in format '{number}/{unit}' (e.g., '10/second')
    """
    key: str
    limit: str


class StatisticsResponse(BaseModel):
    """
    Model for statistics response.

    Attributes:
        key (str): The key the statistics are for
        hourly_calls (Dict[str, int]): Hourly call counts indexed by timestamp
        daily_calls (Dict[str, int]): Daily call counts indexed by timestamp
        current_rate_limit (str): The current rate limit for this key
    """
    key: str
    hourly_calls: Dict[str, int]
    daily_calls: Dict[str, int]
    current_rate_limit: str


# Helper functions
def get_rate_limit(key: str) -> str:
    """
    Get the rate limit for a specific key.

    Checks if a custom rate limit exists for the key, and if not,
    returns the default rate limit.

    Args:
        key (str): The key to get the rate limit for

    Returns:
        str: The rate limit in format '{number}/{unit}' (e.g., '10/second')
    """
    if key in rate_limits:
        return rate_limits[key]
    return DEFAULT_RATE_LIMIT


def set_rate_limit(key: str, limit: str) -> None:
    """
    Set the rate limit for a specific key.

    Args:
        key (str): The key to set the rate limit for
        limit (str): The rate limit in format '{number}/{unit}' (e.g., '10/second')
    """
    rate_limits[key] = limit


def increment_stats(key: str) -> None:
    """
    Increment usage statistics for a key.

    Updates both hourly and daily statistics for the given key.
    Creates new counters if they don't exist yet.

    Args:
        key (str): The key to increment statistics for
    """
    now = datetime.now()
    # Format: YYYY-MM-DD-HH
    hourly_timestamp = now.strftime('%Y-%m-%d-%H')
    # Format: YYYY-MM-DD
    daily_timestamp = now.strftime('%Y-%m-%d')

    # Update hourly statistics
    if hourly_timestamp not in stats_storage[key]['hourly']:
        stats_storage[key]['hourly'][hourly_timestamp] = 0
    stats_storage[key]['hourly'][hourly_timestamp] += 1

    # Update daily statistics
    if daily_timestamp not in stats_storage[key]['daily']:
        stats_storage[key]['daily'][daily_timestamp] = 0
    stats_storage[key]['daily'][daily_timestamp] += 1


def get_stats(key: str) -> dict:
    """
    Get usage statistics for a key.

    Retrieves hourly statistics for the last 24 hours and
    daily statistics for the last 30 days.

    Args:
        key (str): The key to get statistics for

    Returns:
        dict: A dictionary containing hourly and daily call counts
    """
    result = {'hourly_calls': {}, 'daily_calls': {}}
    now = datetime.now()

    # Only return data for the last 24 hours and 30 days
    if key in stats_storage:
        # Filter hourly data for the last 24 hours
        for hour in range(24):
            timestamp = (now - timedelta(hours=hour)).strftime('%Y-%m-%d-%H')
            if timestamp in stats_storage[key]['hourly']:
                result['hourly_calls'][timestamp] = stats_storage[key]['hourly'][timestamp]

        # Filter daily data for the last 30 days
        for day in range(30):
            timestamp = (now - timedelta(days=day)).strftime('%Y-%m-%d')
            if timestamp in stats_storage[key]['daily']:
                result['daily_calls'][timestamp] = stats_storage[key]['daily'][timestamp]

    return result


# Dependency for rate limit checking
async def check_rate_limit(request: Request, key: str):
    """
    Check if key is within rate limit and update statistics.

    This dependency is used by endpoints to track usage statistics
    regardless of whether the rate limit is exceeded.

    Args:
        request (Request): The FastAPI request object
        key (str): The key to check the rate limit for

    Returns:
        str: The key that was checked
    """
    # This updates the statistics regardless of rate limit status
    increment_stats(key)
    # The actual rate limiting is handled by the decorator
    return key


# Routes
@app.get('/check/{key}')
@limiter.limit(get_rate_limit)  # Use the function directly without lambda
async def check_rate_limit_endpoint(
    request: Request,
    key: str = Depends(check_rate_limit),
):
    """
    Check if a key is within its rate limit and return the status.

    This endpoint will return a 429 Too Many Requests response if
    the rate limit is exceeded, or a 200 OK response if within limits.

    Args:
        request (Request): The FastAPI request object
        key (str): The key to check, extracted from path parameter

    Returns:
        dict: A dictionary containing status information
    """
    return {
        'status': 'ok',
        'key': key,
        'rate_limit': get_rate_limit(key),
        'timestamp': datetime.now().isoformat(),
    }


@app.post('/configure', status_code=200)
async def configure_rate_limit(config: RateLimitConfig):
    """
    Configure the rate limit for a specific key.

    This endpoint allows setting a custom rate limit for a key.

    Args:
        config (RateLimitConfig): The configuration containing key and limit

    Returns:
        dict: A dictionary confirming the configuration
    """
    set_rate_limit(config.key, config.limit)
    return {
        'status': 'ok',
        'key': config.key,
        'rate_limit': config.limit,
    }


@app.get('/stats/{key}')
async def get_statistics(key: str):
    """
    Get usage statistics for a specific key.

    Returns hourly statistics for the last 24 hours and
    daily statistics for the last 30 days.

    Args:
        key (str): The key to get statistics for

    Returns:
        dict: A dictionary containing usage statistics
    """
    stats = get_stats(key)
    return {
        'key': key,
        'hourly_calls': stats['hourly_calls'],
        'daily_calls': stats['daily_calls'],
        'current_rate_limit': get_rate_limit(key),
    }


@app.get('/health')
async def health_check():
    """
    Health check endpoint.

    This endpoint is used to verify that the service is running correctly.
    It always returns a 200 OK response with a timestamp.

    Returns:
        dict: A dictionary indicating service health
    """
    return {
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
    }


if __name__ == '__main__':
    # Run the application with uvicorn when script is executed directly
    import uvicorn
    # Get port from environment variable or use default 8000
    port = int(os.getenv('PORT', '8000'))
    uvicorn.run(app, host='0.0.0.0', port=port)
