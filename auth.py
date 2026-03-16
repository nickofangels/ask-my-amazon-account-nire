from dotenv import load_dotenv
import os

load_dotenv()

CREDENTIALS = {
    'refresh_token': os.getenv('SP_API_REFRESH_TOKEN'),
    'lwa_app_id': os.getenv('SP_API_CLIENT_ID'),
    'lwa_client_secret': os.getenv('SP_API_CLIENT_SECRET'),
}

MARKETPLACE = os.getenv('SP_API_MARKETPLACE_ID', 'ATVPDKIKX0DER')

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
    missing = [k for k, v in CREDENTIALS.items() if not v]
    if not MARKETPLACE:
        missing.append('SP_API_MARKETPLACE_ID')
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}\n"
            "Copy .env.example to .env and fill in your SP-API credentials."
        )
