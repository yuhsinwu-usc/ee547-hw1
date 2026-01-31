## Retry Strategy

If the response is successful responses (2xx) or client errors (4xx), no retries will be made.

Retry requests only for: server errors (5xx), timeouts, connection errors and unexpected exceptions.

Each URL is attempted up to MAX_RETRIES times.
After all retries are exhausted, on_max_retries is called.

Retries use exponential backoff with jitter.
A small random jitter is added to prevent synchronized retries.

Before each retry, the client logs the retry event using on_retry.