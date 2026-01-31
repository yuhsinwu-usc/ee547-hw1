import json
import random
import time
import datetime as dt
from typing import Optional, Dict, Any
import urllib.request
import urllib.error
import socket
from url_provider import URLProvider, ResponseValidator

def utc_iso8601() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

class ResponseHandler:

    def __init__(self, log_path: str = "output.log", validator: Optional[Any] = None):
        self.log_path = log_path
        self.validator = validator

        with open(self.log_path, "w", encoding="utf-8") as f:
            f.write("")

    def _record_callback(self, url: str, callback_name: str) -> None:
        if self.validator is not None:
            self.validator.add_callback(url, callback_name)

    def _log_file_info(self, event: str, **infos: Any) -> None:
        line = {"timestamp": utc_iso8601(), "event": event, **infos}
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(line) + "\n")

    def on_success(self, url: str, status: int, body: bytes, latency_ms: float) -> None:
        self._record_callback(url, "on_success")
        self._log_file_info("success", url=url, status=status, latency_ms=latency_ms)

    def on_client_error(self, url: str, status: int, body: bytes) -> None:
        self._record_callback(url, "on_client_error")
        self._log_file_info("client_error", url=url, status=status)

    def on_server_error(self, url: str, status: int, attempt: int) -> None:
        self._record_callback(url, "on_server_error")
        self._log_file_info("server_error", url=url, status=status, attempt=attempt)

    def on_timeout(self, url: str, attempt: int, timeout_sec: float) -> None:
        self._record_callback(url, "on_timeout")
        self._log_file_info("timeout", url=url, attempt=attempt, timeout_sec=timeout_sec)

    def on_connection_error(self, url: str, attempt: int, error: str) -> None:
        self._record_callback(url, "on_connection_error")
        self._log_file_info("connection_error", url=url, attempt=attempt, error=error)

    def on_slow_response(self, url: str, latency_ms: float) -> None:
        self._record_callback(url, "on_slow_response")
        self._log_file_info("slow_response", url=url, latency_ms=latency_ms)

    def on_retry(self, url: str, attempt: int, wait_ms: float, reason: str) -> None:
        self._record_callback(url, "on_retry")
        self._log_file_info("retry", url=url, attempt=attempt, wait_ms=wait_ms, reason=reason)

    def on_body_match(self, url: str, keyword: str) -> None:
        self._record_callback(url, "on_body_match")
        self._log_file_info("body_match", url=url, keyword=keyword)

    def on_max_retries(self, url: str, attempts: int, last_error: str) -> None:
        self._record_callback(url, "on_max_retries")
        self._log_file_info("max_retries", url=url, attempts=attempts, last_error=last_error)

class RobustHTTPClient:
    # Example values; adjust as needed
    SLOW_THRESHOLD_MS = 500.0
    MAX_RETRIES = 3
    BASE_TIMEOUT_SEC = 5.0
    INITIAL_BACKOFF_MS = 100.0
    MAX_BACKOFF_MS = 5000.0
    BACKOFF_MULTIPLIER = 2.0
    # Arbitrary; adjust to match test data
    MONITORED_KEYWORDS = ["foo", "bar", "baz"]

    def __init__(self, handler: ResponseHandler):
        self.handler = handler
        
    def calculate_backoff(self, attempt: int) -> float:
        """
        Calculate backoff delay in milliseconds.

        Args:
            attempt: Retry attempt number (0-indexed)

        Returns:
            Delay in milliseconds

        Formula:
            base_delay = INITIAL_BACKOFF_MS * (BACKOFF_MULTIPLIER ** attempt)
            jitter = random.uniform(0, 0.1 * base_delay)
            delay = min(base_delay + jitter, MAX_BACKOFF_MS)
        """
        base_delay = self.INITIAL_BACKOFF_MS * (self.BACKOFF_MULTIPLIER ** attempt)
        jitter = random.uniform(0, 0.1 * base_delay)
        return min(base_delay + jitter, self.MAX_BACKOFF_MS)

    def fetch(self, url: str) -> bool:
        """
        Fetch URL with retry logic.

        Returns:
            True if eventually successful (2xx), False otherwise.

        Behavior:
        - 2xx: Success. Call on_success. If slow, also call on_slow_response.
               Check body for monitored keywords.
        - 4xx: Client error. Call on_client_error. Do NOT retry.
        - 5xx: Server error. Call on_server_error. Retry with backoff.
        - Timeout: Call on_timeout. Retry with backoff.
        - Connection error: Call on_connection_error. Retry with backoff.

        Retry uses exponential backoff:
            wait_ms = min(INITIAL_BACKOFF_MS * (BACKOFF_MULTIPLIER ** attempt), MAX_BACKOFF_MS)

        Before each retry, call on_retry.
        After max retries exhausted, call on_max_retries.
        """
        last_err_reason = ""
        for try_num in range(self.MAX_RETRIES + 1):
            start = time.perf_counter()

            try:
                with urllib.request.urlopen(url, timeout=self.BASE_TIMEOUT_SEC) as resp:
                    body_info = resp.read()
                    status_code = resp.getcode()

                latency_ms =  1000.0 * (time.perf_counter() - start)

                if 200 <= status_code < 300:
                    self.handler.on_success(url, status_code, body_info, latency_ms)
                    body_text = body_info.decode("utf-8", errors="ignore")

                    if latency_ms > self.SLOW_THRESHOLD_MS:
                        self.handler.on_slow_response(url, latency_ms)
                    
                    for keyword in self.MONITORED_KEYWORDS:
                        if keyword in body_text and keyword != "":
                            self.handler.on_body_match(url, keyword)

                    return True

                if 400 <= status_code < 500:
                    self.handler.on_client_error(url, status_code, body_info)
                    
                    return False

                if 500 <= status_code < 600:
                    self.handler.on_server_error(url, status_code, try_num)
                    last_err_reason = f"server_error:{status_code}"

                    if try_num == self.MAX_RETRIES:
                        self.handler.on_max_retries(url, self.MAX_RETRIES, last_err_reason)
                        return False

                    wait_ms = self.calculate_backoff(try_num)
                    self.handler.on_retry(url, try_num, wait_ms, last_err_reason)
                    time.sleep(wait_ms / 1000.0)
                    continue

                unexpected_err_reason = f"unexpected_status:{status_code}"
                
                if try_num == self.MAX_RETRIES:
                    self.handler.on_max_retries(url, self.MAX_RETRIES, unexpected_err_reason)
                    return False
                
                wait_ms = self.calculate_backoff(try_num)
                self.handler.on_retry(url, try_num, wait_ms, unexpected_err_reason)
                time.sleep(wait_ms / 1000.0)
                continue
            
            except urllib.error.HTTPError as exp:
                if exp.code is not None:
                    status_code = int(exp.code)
                else:
                    status_code = 0

                body_info = ""
                if 400 <= status_code < 500:
                    self.handler.on_client_error(url, status_code, body_info)
                    
                    return False

                if 500 <= status_code < 600:
                    self.handler.on_server_error(url, status_code, try_num)
                    last_err_reason = f"server_error:{status_code}"

                    if try_num == self.MAX_RETRIES:
                        self.handler.on_max_retries(url, self.MAX_RETRIES, last_err_reason)
                        return False

                    wait_ms = self.calculate_backoff(try_num)
                    self.handler.on_retry(url, try_num, wait_ms, reason=last_err_reason)
                    time.sleep(wait_ms / 1000.0)
                    continue

                unexpected_err_reason = f"http_error:{status_code}"
                
                if try_num == self.MAX_RETRIES:
                    self.handler.on_max_retries(url, self.MAX_RETRIES, unexpected_err_reason)
                    return False
                
                wait_ms = self.calculate_backoff(try_num)
                self.handler.on_retry(url, try_num, wait_ms, unexpected_err_reason)
                time.sleep(wait_ms / 1000.0)
                continue
            
            except (socket.timeout, TimeoutError):
                self.handler.on_timeout(url, try_num, self.BASE_TIMEOUT_SEC)
                last_err_reason = f"timeout"

                if try_num == self.MAX_RETRIES:
                    self.handler.on_max_retries(url, self.MAX_RETRIES, last_err_reason)
                    return False

                wait_ms = self.calculate_backoff(try_num)
                self.handler.on_retry(url, try_num, wait_ms, last_err_reason)
                time.sleep(wait_ms / 1000.0)
                continue

            except urllib.error.URLError as exp:
                err_info = str(exp)
                self.handler.on_connection_error(url, try_num, err_info)
                last_err_reason = f"connection_error:{err_info}"

                if try_num == self.MAX_RETRIES:
                    self.handler.on_max_retries(url, self.MAX_RETRIES, last_err_reason)
                    return False

                wait_ms = self.calculate_backoff(try_num)
                self.handler.on_retry(url, try_num, wait_ms, "connection_error")
                time.sleep(wait_ms / 1000.0)
                continue

            except Exception as exp:
                err_info = str(exp)
                self.handler.on_connection_error(url, try_num, err_info)
                last_err_reason = f"connection_error:{err_info}"

                if try_num == self.MAX_RETRIES:
                    self.handler.on_max_retries(url, self.MAX_RETRIES, last_err_reason)
                    return False

                wait_ms = self.calculate_backoff(try_num)
                self.handler.on_retry(url, try_num, wait_ms, reason="exception")
                time.sleep(wait_ms / 1000.0)
                continue
        
        final_err_reason = last_err_reason or "unknown"
        self.handler.on_max_retries(url, self.MAX_RETRIES, final_err_reason)
        return False

    def fetch_all(self, provider: URLProvider) -> dict:
        """
        Fetch all URLs from provider.

        Returns:
            Summary statistics dict.
        """
        total_urls = 0
        successful = 0
        failed = 0
        total_requests = 0
        retries = 0
        avg_latency_ms = 0.0
        slow_responses = 0
        by_status: Dict[str, int] = {}
        by_error: Dict[str, int] = {
            "timeout": 0,
            "connection": 0
        }
        sum_latency_ms = 0.0
        success_latency_count = 0

        while True:
            url = provider.next_url()
            if url is None:
                break
            total_urls += 1

            start_fetch = self.fetch(url)
            if start_fetch:
                successful += 1
            else:
                failed += 1

            url_behavior = provider.get_behavior(url)
            if url_behavior.status_code is not None and url_behavior != "":
                status = str(url_behavior.status_code)
                by_status[status] = by_status.get(status, 0) + 1
            elif url_behavior.error_type and url_behavior != "":
                by_error[url_behavior.error_type] = by_error.get(url_behavior.error_type, 0) + 1

        if success_latency_count > 0:
            avg_latency_ms = sum_latency_ms / success_latency_count
            
        return {
            "total_urls": total_urls,
            "successful": successful,
            "failed": failed,
            "total_requests": total_requests,
            "retries": retries,
            "avg_latency_ms": avg_latency_ms,
            "slow_responses": slow_responses,
            "by_status": by_status,
            "by_error": by_error,
        }
