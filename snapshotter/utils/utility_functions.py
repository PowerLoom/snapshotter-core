import asyncio
from functools import wraps

from snapshotter.utils.default_logger import logger


def acquire_bounded_semaphore(fn):
    """
    A decorator function that acquires a bounded semaphore before executing the decorated function and releases it
    after the function is executed. This decorator is intended to be used with async functions.

    Args:
        fn (callable): The async function to be decorated.

    Returns:
        callable: The decorated async function.

    Raises:
        Any exception that may occur during the execution of the decorated function.
    """
    @wraps(fn)
    async def wrapped(self, *args, **kwargs):
        """
        Wrapper function that handles semaphore acquisition and release.

        Args:
            self: The instance of the class containing the decorated method.
            *args: Variable length argument list for the decorated function.
            **kwargs: Arbitrary keyword arguments for the decorated function.

        Returns:
            Any: The result of the decorated function.

        Raises:
            Exception: Any exception that occurs during the execution of the decorated function.
        """
        # Extract the semaphore from the keyword arguments
        sem: asyncio.BoundedSemaphore = kwargs['semaphore']
        
        # Acquire the semaphore
        await sem.acquire()
        
        result = None
        try:
            # Execute the decorated function
            result = await fn(self, *args, **kwargs)
        except Exception as e:
            # Log any exceptions that occur during execution
            logger.opt(exception=True).error('Error in asyncio semaphore acquisition decorator: {}', e)
            raise  # Re-raise the exception after logging
        finally:
            # Ensure the semaphore is released, even if an exception occurred
            sem.release()
        
        return result
    
    return wrapped
