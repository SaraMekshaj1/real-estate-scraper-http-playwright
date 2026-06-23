from __future__ import annotations


class ScraperException(Exception):
    """Base class for all scraper-specific exceptions."""


class TranslationQuotaExceeded(ScraperException):
    """
    Raised when the translation provider signals a quota or rate-limit error.

    WHY a dedicated exception rather than a generic RuntimeError:
      - The pipeline can catch *this* type specifically and stop gracefully
        without masking unrelated errors (network failures, parse bugs, etc.).
      - Callers can distinguish "we ran out of quota — safe to resume later"
        from "something unexpected broke".
      - Keeps the translation abstraction honest: quota exhaustion is a
        *known*, *expected* failure mode for any metered API, not an accident.

    Usage (in a translator adapter):
        raise TranslationQuotaExceeded(
            f"Google Translate quota hit after {n} characters"
        )

    Usage (in the pipeline):
        except TranslationQuotaExceeded:
            logger.warning("Quota exceeded — stopping cleanly after current batch")
            break
    """


class TranslationError(ScraperException):
    """
    Raised for non-recoverable translation failures (bad credentials, etc.).
    Distinct from quota errors so the pipeline can apply different strategies.
    """