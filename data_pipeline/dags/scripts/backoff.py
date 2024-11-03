import time
import logging
from random import uniform
from functools import wraps
from typing import Optional, Callable, Any

def exponential_backoff(
    max_retries: int = 10,
    base_delay: float = 1,
    max_delay: float = 32,
    exponential_base: float = 2,
    jitter: bool = True
) -> Callable:
    """
    Decorator that implements exponential backoff retry logic.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential calculation
        jitter: Whether to add random jitter to delay
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """
            Internal wrapper function that implements the retry logic.
            """
            
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries > max_retries:
                        logging.error(f"Max retries ({max_retries}) exceeded. Last error: {str(e)}")
                        raise
                    
                    delay = min(base_delay * (exponential_base ** (retries - 1)), max_delay)
                    if jitter:
                        delay = delay * uniform(0.5, 1.5)
                    
                    logging.warning(
                        f"Attempt {retries}/{max_retries} failed: {str(e)}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    
                    time.sleep(delay)
        return wrapper
    return decorator