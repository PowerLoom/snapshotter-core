from enum import Enum
from typing import List
from typing import Optional

from pydantic import BaseModel


class UserStatusEnum(str, Enum):
    """Enumeration of possible user statuses."""
    active = 'active'
    inactive = 'inactive'


class AddApiKeyRequest(BaseModel):
    """Model for API key addition request."""
    api_key: str


class AppOwnerModel(BaseModel):
    """Model representing an application owner."""
    email: str
    rate_limit: str
    active: UserStatusEnum
    callsCount: int = 0  # Number of API calls made
    throttledCount: int = 0  # Number of times the user has been throttled
    next_reset_at: int  # Timestamp for the next rate limit reset


class UserAllDetailsResponse(AppOwnerModel):
    """Model for responding with all user details, including API keys."""
    active_api_keys: List[str]
    revoked_api_keys: List[str]


class AuthCheck(BaseModel):
    """Model for authentication check results."""
    authorized: bool = False
    api_key: str
    reason: str = ''  # Reason for authorization failure, if any
    owner: Optional[AppOwnerModel] = None  # Owner details if authorized


class RateLimitAuthCheck(AuthCheck):
    """Model for authentication check results with rate limiting information."""
    rate_limit_passed: bool = False
    retry_after: int = 1  # Seconds to wait before retrying
    violated_limit: str  # The rate limit that was violated
    current_limit: str  # The current rate limit for the user
