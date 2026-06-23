from __future__ import annotations

from urllib.parse import (
    ParseResult,
    SplitResult,
    urlencode,
    urlparse,
    urlsplit,
    urlunsplit,
    parse_qsl,
    urlunparse,
)

_STRIP_PARAMS: frozenset[str] = frozenset({
    # Google
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "utm_source_platform", "utm_creative_format", "utm_marketing_tactic",
    "gclid", "gclsrc", "gbraid", "wbraid",
    # Meta / Facebook
    "fbclid", "fb_action_ids", "fb_action_types", "fb_ref", "fb_source",
    # Microsoft
    "msclkid",
    # Generic tracking
    "ref", "source", "campaign", "affiliate", "partner",
    "mc_cid", "mc_eid",          # Mailchimp
    "igshid",                    # Instagram
    "twclid",                    # Twitter/X
    "_ga", "_gl",                # Google Analytics
    "zanpid",                    # Zanox
    "origin",                    # sometimes used as tracking
})

# Schemes where we know the default port so we can strip it
_DEFAULT_PORTS: dict[str, int] = {"http": 80, "https": 443}


def canonicalize_url(url: str) -> str:
    if not url or not url.strip():
        return url

    try:
        parsed = urlsplit(url.strip())
    except Exception:
        return url

    # 1. Lowercase scheme and host
    scheme = parsed.scheme.lower()
    host   = parsed.netloc.lower()

    # 2. Strip default port
    if ":" in host:
        hostname, port_str = host.rsplit(":", 1)
        try:
            if int(port_str) == _DEFAULT_PORTS.get(scheme):
                host = hostname
        except ValueError:
            pass  # non-numeric port — leave as-is

    # 3. Strip tracking params + 4. sort remaining
    clean_params = sorted(
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=False)
        if k.lower() not in _STRIP_PARAMS
    )
    query = urlencode(clean_params)

    # 5. Drop fragment entirely
    # 6. Remove trailing slash (but keep bare "/" as-is)
    path = parsed.path.rstrip("/") or "/"

    canonical = urlunsplit((scheme, host, path, query, ""))
    return canonical


def canonicalize_url_safe(url: str) -> str:
    try:
        return canonicalize_url(url)
    except Exception:
        return url