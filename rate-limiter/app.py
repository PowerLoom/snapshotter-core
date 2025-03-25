import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Request
from slowapi import _rate_limit_exceeded_handler
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
load_dotenv()

app = FastAPI(title='Rate Limiter')

# Initialize rate limiter
limiter = Limiter(key_func=lambda: 'global')  # Using a global limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
RATE_LIMIT = os.getenv('RPC_RATE_LIMIT', '10')  # Default to 10 if not set

# Construct the rate limit string using the environment variable
rate_limit_string = f'{RATE_LIMIT}/second'


@app.get('/check/{key}')
@limiter.limit(rate_limit_string)
async def check_rate_limit(request: Request, key: str):
    """
    Check if key is within rate limit configured via RPC_RATE_LIMIT environment variable
    """
    return {
        'status': 'ok',
        'key': key,
        'rate_limit': rate_limit_string,
    }

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
