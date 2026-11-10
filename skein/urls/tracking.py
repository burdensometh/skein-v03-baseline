"""What canonicalisation rules version 2 removes: tracking parameters and path session ids.

Marketing and analytics systems append parameters that identify a click,
a campaign or a mailing, never the resource itself. Two links that differ
only in them address the same document, so rules version 2 drops them.
Session identifiers that application servers put into the path
(``/cart;jsessionid=0A1B``) are removed for the same reason.
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass

from skein.urls.percent import percent_decode

# Shell-style patterns matched against the decoded, lower-cased parameter name.
TRACKING_PARAMS: tuple[str, ...] = (
    "utm_*",          # Google Analytics campaign parameters
    "gclid", "gclsrc", "dclid", "gbraid", "wbraid",  # Google Ads click ids
    "fbclid",         # Facebook
    "msclkid",        # Microsoft Ads
    "twclid",         # Twitter / X
    "ttclid",         # TikTok
    "li_fat_id",      # LinkedIn
    "yclid", "ysclid",  # Yandex
    "igshid",         # Instagram shares
    "mc_cid", "mc_eid",  # Mailchimp
    "_hsenc", "_hsmi", "hsctatracking",  # HubSpot
    "mkt_tok",        # Marketo
    "oly_anon_id", "oly_enc_id",  # Omeda
    "vero_id", "vero_conv",  # Vero
    "_ga", "_gl",     # Google cross-domain linker
    "s_cid",          # Adobe Analytics
    "ref_src", "ref_url",  # embedded-tweet referrers
)

# Path parameters carrying a server session, e.g. ``;jsessionid=...``.
SESSION_PATH_PARAMS: tuple[str, ...] = ("jsessionid", "phpsessid", "aspsessionid", "sid", "cfid", "cftoken")
_PATH_PARAM_RE = re.compile(r";([^/;=]+)=[^/;]*")


@dataclass(frozen=True)
class TrackingFilter:
    """Recognises tracking parameters: the built-in catalogue plus ``extra`` patterns."""

    extra: tuple[str, ...] = ()

    @property
    def patterns(self) -> tuple[str, ...]:
        return TRACKING_PARAMS + tuple(p.lower() for p in self.extra)

    def is_tracking(self, raw_name: str) -> bool:
        name = percent_decode(raw_name, plus_as_space=True).lower()
        return any(fnmatch.fnmatchcase(name, pattern) for pattern in self.patterns)


def strip_session_params(path: str) -> str:
    """Remove ``;name=value`` path parameters whose name is a known session id (any segment)."""
    if ";" not in path:
        return path

    def keep(match: re.Match[str]) -> str:
        name = percent_decode(match.group(1)).lower()
        return "" if name in SESSION_PATH_PARAMS else match.group(0)

    return _PATH_PARAM_RE.sub(keep, path)
