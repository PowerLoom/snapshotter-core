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

load_dotenv()

# Configuration
DEFAULT_RATE_LIMIT = os.getenv('DEFAULT_RATE_LIMIT', '10/second')

# Initialize FastAPI app
app = FastAPI(title='Rate Limiter')

# In-memory statistics storage
# Format: {key: {'hourly': {timestamp: count}, 'daily': {timestamp: count}}}
stats_storage = defaultdict(lambda: {'hourly': {}, 'daily': {}})

# In-memory rate limit storage
# Format: {key: rate_limit_string}
rate_limits = {}


# Initialize rate limiter with key function that uses the key from path params
def get_key_func(request: Request) -> str:
    """Extract the key from path parameters or use IP as fallback"""
    if hasattr(request, 'path_params') and 'key' in request.path_params:
        return request.path_params['key']
    return get_remote_address(request)


limiter = Limiter(key_func=get_key_func)  # Use our custom key function
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# Models
class RateLimitConfig(BaseModel):
    key: str
    limit: str


class StatisticsResponse(BaseModel):
    key: str
    hourly_calls: Dict[str, int]
    daily_calls: Dict[str, int]
    current_rate_limit: str


# Helper functions
def get_rate_limit(key: str) -> str:
    """Get the rate limit for a specific key"""
    if key in rate_limits:
        return rate_limits[key]
    return DEFAULT_RATE_LIMIT


def set_rate_limit(key: str, limit: str) -> None:
    """Set the rate limit for a specific key"""
    rate_limits[key] = limit


def increment_stats(key: str) -> None:
    """Increment usage statistics for a key"""
    now = datetime.now()
    hourly_timestamp = now.strftime('%Y-%m-%d-%H')
    daily_timestamp = now.strftime('%Y-%m-%d')

    # In-memory storage
    if hourly_timestamp not in stats_storage[key]['hourly']:
        stats_storage[key]['hourly'][hourly_timestamp] = 0
    stats_storage[key]['hourly'][hourly_timestamp] += 1

    if daily_timestamp not in stats_storage[key]['daily']:
        stats_storage[key]['daily'][daily_timestamp] = 0
    stats_storage[key]['daily'][daily_timestamp] += 1


def get_stats(key: str) -> dict:
    """Get usage statistics for a key"""
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
    """Check if key is within rate limit and update statistics"""
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
    Check if a key is within its rate limit and return the status
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
    Configure the rate limit for a specific key
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
    Get usage statistics for a specific key
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
    Health check endpoint
    """
    return {
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
    }


if __name__ == '__main__':
    import uvicorn
    port = int(os.getenv('PORT', '8000'))
    uvicorn.run(app, host='0.0.0.0', port=port)
