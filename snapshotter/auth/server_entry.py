import time

from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from redis import asyncio as aioredis

from snapshotter.auth.helpers.data_models import AddApiKeyRequest
from snapshotter.auth.helpers.data_models import AppOwnerModel
from snapshotter.auth.helpers.data_models import UserAllDetailsResponse
from snapshotter.auth.helpers.redis_conn import RedisPoolCache
from snapshotter.auth.helpers.redis_keys import all_users_set
from snapshotter.auth.helpers.redis_keys import api_key_to_owner_key
from snapshotter.auth.helpers.redis_keys import user_active_api_keys_set
from snapshotter.auth.helpers.redis_keys import user_details_htable
from snapshotter.auth.helpers.redis_keys import user_revoked_api_keys_set
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger

# Setup logging
api_logger = default_logger.bind(module='AuthAPI')

# Setup CORS origins
origins = ['*']
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.on_event('startup')
async def startup_boilerplate():
    """
    Initialize the Redis connection pool and core settings on application startup.
    """
    app.state.aioredis_pool = RedisPoolCache(pool_size=100)
    await app.state.aioredis_pool.populate()
    app.state.redis_pool = app.state.aioredis_pool._aioredis_pool
    app.state.core_settings = settings


@app.post('/user')
async def create_update_user(
    request: Request,
    user_cu_request: AppOwnerModel,
    response: Response,
):
    """
    Create a new user or update an existing user's information in the Redis hash table.

    Args:
        request (Request): The FastAPI request object.
        user_cu_request (AppOwnerModel): The user data to be created or updated.
        response (Response): The FastAPI response object.

    Returns:
        dict: A dictionary indicating the success status of the operation.
    """
    redis_conn: aioredis.Redis = request.app.state.redis_pool
    try:
        # Add user email to the set of all users
        await redis_conn.sadd(
            all_users_set(),
            user_cu_request.email,
        )

        # If the user is new, set the next reset time
        if not await redis_conn.sismember(
            all_users_set(),
            user_cu_request.email,
        ):
            user_cu_request.next_reset_at = int(time.time()) + 86400

        # Store user details in the hash table
        user_details = user_cu_request.dict()
        await redis_conn.hset(
            name=user_details_htable(user_cu_request.email),
            mapping=user_details,
        )
    except Exception as e:
        api_logger.opt(exception=settings.logs.debug_mode).error('{}', e)
        return {'success': False}
    else:
        return {'success': True}


@app.post('/user/{email}/api_key')
async def add_api_key(
    api_key_request: AddApiKeyRequest,
    email: str,
    request: Request,
    response: Response,
):
    """
    Add a new API key for a user.

    Args:
        api_key_request (AddApiKeyRequest): The API key to be added.
        email (str): The email of the user.
        request (Request): The FastAPI request object.
        response (Response): The FastAPI response object.

    Returns:
        dict: A dictionary indicating the success status of the operation and any error messages.
    """
    redis_conn: aioredis.Redis = request.app.state.redis_pool
    if not await redis_conn.sismember(all_users_set(), email):
        return {'success': False, 'error': 'User does not exist'}

    async with redis_conn.pipeline(transaction=True) as p:
        # Add the API key to the user's active keys set and map it to the user's email
        await p.sadd(
            user_active_api_keys_set(email),
            api_key_request.api_key,
        ).set(api_key_to_owner_key(api_key_request.api_key), email).execute()
    return {'success': True}


@app.delete('/user/{email}/api_key')
async def revoke_api_key(
    api_key_request: AddApiKeyRequest,
    email: str,
    request: Request,
    response: Response,
):
    """
    Revoke an API key for a user.

    Args:
        api_key_request (AddApiKeyRequest): The API key to be revoked.
        email (str): The email of the user.
        request (Request): The FastAPI request object.
        response (Response): The FastAPI response object.

    Returns:
        dict: A dictionary indicating the success status of the operation and any error messages.
    """
    redis_conn: aioredis.Redis = request.app.state.redis_pool
    if not await redis_conn.sismember(all_users_set(), email):
        return {'success': False, 'error': 'User does not exist'}

    if not await redis_conn.sismember(
        user_active_api_keys_set(email),
        api_key_request.api_key,
    ):
        return {'success': False, 'error': 'API key not active'}
    elif await redis_conn.sismember(
        user_revoked_api_keys_set(email),
        api_key_request.api_key,
    ):
        return {'success': False, 'error': 'API key already revoked'}

    # Move the API key from active to revoked set
    await redis_conn.smove(
        user_active_api_keys_set(email),
        user_revoked_api_keys_set(email),
        api_key_request.api_key,
    )
    return {'success': True}


@app.get('/user/{email}')
async def get_user_details(
    request: Request,
    response: Response,
    email: str,
):
    """
    Retrieve all details for a specific user.

    Args:
        request (Request): The FastAPI request object.
        response (Response): The FastAPI response object.
        email (str): The email of the user.

    Returns:
        dict: A dictionary containing the user's details, including active and revoked API keys.
    """
    redis_conn: aioredis.Redis = request.app.state.redis_pool

    all_details = await redis_conn.hgetall(name=user_details_htable(email))
    if not all_details:
        return {'success': False, 'error': 'User does not exist'}

    active_api_keys = await redis_conn.smembers(
        name=user_active_api_keys_set(email),
    )
    revoked_api_keys = await redis_conn.smembers(
        name=user_revoked_api_keys_set(email),
    )

    return {
        'success': True,
        'data': UserAllDetailsResponse(
            **{k.decode('utf-8'): v.decode('utf-8') for k, v in all_details.items()},
            active_api_keys=[x.decode('utf-8') for x in active_api_keys],
            revoked_api_keys=[x.decode('utf-8') for x in revoked_api_keys],
        ).dict(),
    }


@app.get('/users')
async def get_all_users(
    request: Request,
    response: Response,
):
    """
    Retrieve a list of all users.

    Args:
        request (Request): The FastAPI request object.
        response (Response): The FastAPI response object.

    Returns:
        dict: A dictionary containing a list of all user emails.
    """
    redis_conn: aioredis.Redis = request.app.state.redis_pool
    all_users = await redis_conn.smembers(all_users_set())
    return {
        'success': True,
        'data': [x.decode('utf-8') for x in all_users],
    }
