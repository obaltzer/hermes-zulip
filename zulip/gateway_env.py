"""Zulip env -> gateway config bridge (``PlatformEntry.env_overrides``)."""

from gateway.config import Platform
from gateway.platform_env import EnvCred, home_step

ENV_ENABLE_CREDENTIALS = ("ZULIP_SITE", "ZULIP_EMAIL", "ZULIP_API_KEY")
ENV_OVERRIDES = (
    EnvCred(
        Platform("zulip"), ("ZULIP_SITE", "ZULIP_EMAIL", "ZULIP_API_KEY"), token="ZULIP_API_KEY",
        fixed=(("site", "ZULIP_SITE"), ("email", "ZULIP_EMAIL")),
    ),
    home_step(Platform("zulip"), "ZULIP_HOME_CHANNEL"),
)

# Authorization env names (``PlatformEntry`` fields). Core reads them from this file when the
# live entry lacks them: a user plugin overriding this platform, safe mode, the plugin disabled.
AUTHZ = {
    "allowed_users_env": "ZULIP_ALLOWED_USERS",
    "allow_all_env": "ZULIP_ALLOW_ALL_USERS",
    "allow_bots_env": "ZULIP_ALLOW_BOTS",
    "allowlist_suppresses_pairing": True,
}
