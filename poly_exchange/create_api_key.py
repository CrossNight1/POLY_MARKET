## GET Key trong export key in account Polymarket

import os
import sys
import json
import time
from typing import Dict, Any

import requests
from eth_account import Account

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

LIBRARY_ROOT = os.path.join(os.path.dirname(ROOT), "Library", "polymarket", "py-clob-client")
if os.path.isdir(LIBRARY_ROOT) and LIBRARY_ROOT not in sys.path:
    sys.path.insert(0, LIBRARY_ROOT)

from logger import logger_polymarket

try:
    from exchanges.polymarket_exchange.derive_api_key import derive_api_key as _derive_existing_api_key
except Exception:
    _derive_existing_api_key = None

CLOB_API_URL = "https://clob.polymarket.com"
CHAIN_ID = 137


def _normalize_private_key(private_key: str) -> str:
    key = (private_key or "").strip()
    if not key.startswith('0x'):
        key = '0x' + key
    return key


def _create_or_derive_with_client(private_key: str, nonce: int):
    try:
        from py_clob_client.client import ClobClient
    except ImportError:
        return None
    try:
        normalized_key = _normalize_private_key(private_key)
        client = ClobClient(CLOB_API_URL, key=normalized_key, chain_id=CHAIN_ID)
        creds = client.create_or_derive_api_creds(nonce)
        if not creds:
            return None
        data = {
            'key': creds.api_key,
            'secret': creds.api_secret,
            'passphrase': creds.api_passphrase,
            'address': client.signer.address()
        }
        return {
            'success': True,
            'data': data
        }
    except Exception as exc:
        if logger_polymarket:
            logger_polymarket.error(f'_create_or_derive_with_client error: {exc}')
        return None


def _derive_existing_credentials(private_key: str, nonce: int):
    if _derive_existing_api_key is None:
        return None
    try:
        return _derive_existing_api_key(private_key, nonce)
    except Exception as exc:
        if logger_polymarket:
            logger_polymarket.error(f'_derive_existing_credentials error: {exc}')
        return None


def _build_l1_headers(private_key: str, nonce: int):
    normalized_key = _normalize_private_key(private_key)
    try:
        from py_clob_client.signer import Signer
        from py_clob_client.headers.headers import create_level_1_headers
        signer = Signer(normalized_key, CHAIN_ID)
        raw_headers = create_level_1_headers(signer, nonce)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        for key, value in raw_headers.items():
            headers[key] = str(value)
        headers.setdefault('POLY_ADDRESS', signer.address())
        return headers, signer.address()
    except ImportError:
        sig_data = create_l1_signature(normalized_key, nonce)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'POLY_ADDRESS': sig_data['address'],
            'POLY_SIGNATURE': sig_data['signature'],
            'POLY_TIMESTAMP': str(sig_data['timestamp']),
            'POLY_NONCE': str(sig_data['nonce'])
        }
        return headers, sig_data['address']


def create_l1_signature(private_key: str, nonce: int = 0) -> Dict[str, Any]:
    """
    Creates an L1 EIP-712 signature for Polymarket API key creation.
    
    Args:
        private_key (str): Polygon private key (with or without 0x prefix)
        nonce (int): Nonce for the signature (default: 0)
        
    Returns:
        dict: Dictionary containing signature and related data
    """
    private_key = _normalize_private_key(private_key)
    
    account = Account.from_key(private_key)
    timestamp = str(int(time.time()))
    
    domain = {
        "name": "ClobAuthDomain",
        "version": "1",
        "chainId": CHAIN_ID,
    }
    
    types = {
        "ClobAuth": [
            {"name": "address", "type": "address"},
            {"name": "timestamp", "type": "string"},
            {"name": "nonce", "type": "uint256"},
            {"name": "message", "type": "string"},
        ]
    }
    
    value = {
        "address": account.address,
        "timestamp": timestamp,
        "nonce": nonce,
        "message": "This message attests that I control the given wallet",
    }
    
    try:
        from eth_account.messages import encode_typed_data
        msg_data = encode_typed_data(
            domain_data=domain,
            message_types=types,
            message_data=value
        )
    except ImportError:
        from eth_account.messages import encode_structured_data
        msg_data = encode_structured_data({
            "types": types,
            "primaryType": "ClobAuth",
            "domain": domain,
            "message": value
        })
    
    signed_message = account.sign_message(msg_data)
    
    return {
        "address": account.address,
        "signature": signed_message.signature.hex(),
        "timestamp": timestamp,
        "nonce": nonce
    }


def get_api_key(private_key: str, nonce: int = 0) -> Dict[str, Any]:
    """
    Creates an API key for Polymarket CLOB by making an L1 authenticated request.
    
    Args:
        private_key (str): Polygon private key (with or without 0x prefix)
        nonce (int): Nonce for the signature (default: 0)
        
    Returns:
        dict: API credentials (key, secret, passphrase) or error information
    """
    client_result = _create_or_derive_with_client(private_key, nonce)
    if client_result:
        return client_result
    try:
        headers, address = _build_l1_headers(private_key, nonce)
        url = f"{CLOB_API_URL}/auth/api-key"
        
        if logger_polymarket:
            logger_polymarket.info(f"Sending API key creation request to {url}")
        
        response = requests.post(url, headers=headers, timeout=30)
        
        if logger_polymarket:
            logger_polymarket.info(f"Response status: {response.status_code}")
            logger_polymarket.info(f"Response body: {response.text}")
        
        if response.status_code >= 400:
            error_info = f"HTTP {response.status_code}: {response.text}"
            if response.status_code == 400 and "Could not create api key" in response.text:
                derived = _derive_existing_credentials(private_key, nonce)
                if derived and derived.get('success'):
                    return derived
            if logger_polymarket:
                logger_polymarket.error(f"API error: {error_info}")
            return {
                'success': False,
                'error': error_info,
                'data': {}
            }
        
        result = response.json()
        
        api_key_value = result.get('key') or result.get('apiKey')
        secret_value = result.get('secret') or result.get('apiSecret')
        passphrase_value = result.get('passphrase') or result.get('apiPassphrase')

        if api_key_value and secret_value and passphrase_value:
            logger_polymarket.info("API key created successfully!")
            return {
                'success': True,
                'data': {
                    'key': api_key_value,
                    'secret': secret_value,
                    'passphrase': passphrase_value,
                    'address': address
                }
            }
        else:
            if logger_polymarket:
                logger_polymarket.error(f"Unexpected response: {result}")
            return {
                'success': False,
                'error': result,
                'data': {}
            }
            
    except requests.exceptions.HTTPError as e:
        error_msg = str(e)
        if "401" in error_msg:
            error_msg = (
                "401 Unauthorized - Invalid L1 signature headers. "
                "Possible causes:\n"
                "  1. Invalid or incorrect Polygon private key\n"
                "  2. Ensure you're using a Polygon (not Ethereum mainnet) private key\n"
                "  3. Timestamp/signature mismatch - check system time is correct"
            )
        if logger_polymarket:
            logger_polymarket.error(f"get_api_key error: {error_msg}")
        return {
            'success': False,
            'error': error_msg,
            'data': {}
        }
    except Exception as e:
        error_msg = f"get_api_key error: {e}"
        if logger_polymarket:
            logger_polymarket.error(error_msg)
        return {
            'success': False,
            'error': str(e),
            'data': {}
        }


def save_api_credentials(credentials: Dict[str, str], filename: str = ".polymarket_credentials"):
    """
    Saves API credentials to a file.
    
    Args:
        credentials (dict): API credentials dictionary
        filename (str): Filename to save to (relative to current directory)
    """
    try:
        config_data = {
            'API_KEY': credentials['key'],
            'SECRET_KEY': credentials['secret'],
            'WALLET_ADDRESS': credentials['address'],
            'PASSPHRASE': credentials['passphrase']
        }
        
        with open(filename, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        os.chmod(filename, 0o600)
        
        if logger_polymarket:
            logger_polymarket.info(f"Credentials saved to {filename}")
        return True
    except Exception as e:
        if logger_polymarket:
            logger_polymarket.error(f"save_api_credentials error: {e}")
        return False


def main():
    import getpass
    
    try:
        from py_clob_client.signer import Signer
        print("✓ py-clob-client library found - using official signing\n")
    except ImportError:
        print("⚠ py-clob-client library not found")
        print("  Install it with: pip install py-clob-client\n"
              "  Or try anyway with manual signing (may have compatibility issues)\n")
    
    private_key = getpass.getpass("Enter your Polygon private key (will not be displayed): ").strip()
    
    if not private_key:
        print("Error: Private key cannot be empty")
        return
    
    if not (private_key.startswith('0x') or len(private_key) == 64 or len(private_key) == 66):
        print("Error: Invalid private key format (should be 64 hex chars or 66 with 0x prefix)")
        return
    
    nonce_input = input("Enter nonce to use (default: 0): ").strip()
    try:
        nonce = int(nonce_input) if nonce_input else 0
    except ValueError:
        print("Error: Nonce must be an integer")
        return
    
    try:
        account = Account.from_key(private_key)
        print(f"\nWallet address: {account.address}")
        print("Generating or deriving API key...")
    except Exception as e:
        print(f"Error: Invalid private key - {e}")
        return
    
    result = get_api_key(private_key, nonce)
    
    if result['success']:
        credentials = result['data']
        
        print("\n" + "="*50)
        print("API KEY CREATION SUCCESSFUL!")
        print("="*50)
        print(f"Address:    {credentials['address']}")
        print(f"API Key:    {credentials['key']}")
        print(f"Secret:     {credentials['secret']}")
        print(f"Passphrase: {credentials['passphrase']}")
        print("="*50)

    else:
        error_msg = result.get('error', 'Unknown error')
        print(f"\n✗ Error creating API key: {error_msg}")


if __name__ == "__main__":
    main()
