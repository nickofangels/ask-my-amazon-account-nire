from dotenv import load_dotenv
from sp_api.base import Marketplaces as _Marketplaces
import os

load_dotenv()

CREDENTIALS = {
    'refresh_token': os.getenv('SP_API_REFRESH_TOKEN'),
    'lwa_app_id': os.getenv('SP_API_CLIENT_ID'),
    'lwa_client_secret': os.getenv('SP_API_CLIENT_SECRET'),
    'role_arn': os.getenv('SP_API_ROLE_ARN'),
}

_marketplace_id = (
    os.getenv('SP_API_MARKETPLACE_ID')
    or os.getenv('SP_API_MARKETPLACE_ID_US')
    or 'ATVPDKIKX0DER'
)
MARKETPLACE = next(
    (m for m in _Marketplaces if m.marketplace_id == _marketplace_id),
    _Marketplaces.US,
)
MARKETPLACE_ID = MARKETPLACE.marketplace_id

# ------------------------------------------------------------------
# Usage in any script:
#
#   from auth import CREDENTIALS, MARKETPLACE
#   from sp_api.api import Orders          # or Reports, Catalog, etc.
#
#   client = Orders(credentials=CREDENTIALS, marketplace=MARKETPLACE)
#
# Never re-implement auth logic or inline credentials in a script.
# Always import from this file.
# ------------------------------------------------------------------

def validate():
    """Raise a clear error if any required credential is missing."""
    required = ['refresh_token', 'lwa_app_id', 'lwa_client_secret']
    missing = [k for k in required if not CREDENTIALS.get(k)]
    if not MARKETPLACE:
        missing.append('SP_API_MARKETPLACE_ID')
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}\n"
            "Copy .env.example to .env and fill in your SP-API credentials."
        )
    if not CREDENTIALS.get('role_arn'):
        import warnings
        warnings.warn(
            "SP_API_ROLE_ARN is not set. Brand Analytics reports may fail "
            "if your SP-API app uses IAM role-based auth.",
            stacklevel=2,
        )
