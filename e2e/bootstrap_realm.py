# Runs inside the Zulip container via `manage.py shell`: creates a test realm, an owner and two
# generic bots (one per Hermes profile), and prints their credentials as JSON.
import json

from zerver.actions.create_realm import do_create_realm
from zerver.actions.create_user import do_create_user
from zerver.models import Realm, UserProfile

realm = Realm.objects.filter(string_id="").first() or do_create_realm(string_id="", name="Hermes E2E")


def user(email, full_name, **kwargs):
    existing = UserProfile.objects.filter(delivery_email=email, realm=realm).first()
    return existing or do_create_user(email, kwargs.pop("password", None), realm, full_name, acting_user=None, **kwargs)


owner = user("admin@example.com", "Admin", password="hermes-e2e-pass", role=UserProfile.ROLE_REALM_OWNER)
bob = user("bob@example.com", "Bob", password="hermes-e2e-pass")
hermes = user("hermes-bot@example.com", "Hermes", bot_type=UserProfile.DEFAULT_BOT, bot_owner=owner)
research = user("research-bot@example.com", "Research", bot_type=UserProfile.DEFAULT_BOT, bot_owner=owner)
print("CREDENTIALS=" + json.dumps({
    u.full_name: {"email": u.delivery_email, "api_key": u.api_key, "user_id": u.id} for u in (owner, bob, hermes, research)}))
