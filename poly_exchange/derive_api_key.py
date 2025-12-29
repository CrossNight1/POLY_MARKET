import os
import sys
import json
import time
from typing import Dict, Any

import requests
from web3 import Web3
from eth_account.messages import encode_structured_data

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from logger import logger_polymarket

CLOB_API_URL = "https://clob.polymarket.com"
CHAIN_ID = 137
POLYGON_RPC = "https://polygon-rpc.com"


def create_l1_signature(private_key: str, nonce: int = 0) -> Dict[str, Any]:
    """
    Creates an L1 EIP-712 signature for Polymarket.
    
    Args:
        private_key (str): Polygon private key (with or without 0x prefix)
        nonce (int): Nonce for the signature
        
    Returns:
        dict: Dictionary containing signature and related data
    """
    if not private_key.startswith('0x'):
        private_key = '0x' + private_key
    
    w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
    account = w3.eth.account.from_key(private_key)
    
    timestamp = str(int(time.time() * 1000))
    
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
    
    structured_data = encode_structured_data({
        "types": types,
        "primaryType": "ClobAuth",
        "domain": domain,
        "message": value
    })
    
    signed_message = account.sign_message(structured_data)
    
    return {
        "address": account.address,
        "signature": signed_message.signature.hex(),
        "timestamp": timestamp,
        "nonce": nonce
    }


def derive_api_key(private_key: str, nonce: int = 0) -> Dict[str, Any]:
    """
    Derives an existing API key for an address and nonce.
    
    This recovers API credentials if you've lost them.
    
    Args:
        private_key (str): Polygon private key (with or without 0x prefix)
        nonce (int): Nonce used when creating the API key (default: 0)
        
    Returns:
        dict: API credentials (key, secret, passphrase) or error information
    """
    try:
        logger_polymarket.info("Creating L1 signature for API key derivation...")
        sig_data = create_l1_signature(private_key, nonce)
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'POLY-ADDRESS': sig_data['address'],
            'POLY-SIGNATURE': sig_data['signature'],
            'POLY-TIMESTAMP': sig_data['timestamp'],
            'POLY-NONCE': str(sig_data['nonce'])
        }
        
        url = f"{CLOB_API_URL}/auth/derive-api-key"
        params = {
            'nonce': nonce
        }
        
        logger_polymarket.info(f"Sending API key derivation request to {url}")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        
        if 'key' in result and 'secret' in result and 'passphrase' in result:
            logger_polymarket.info("API key derived successfully!")
            return {
                'success': True,
                'data': {
                    'key': result['key'],
                    'secret': result['secret'],
                    'passphrase': result['passphrase'],
                    'address': sig_data['address']
                }
            }
        else:
            logger_polymarket.error(f"Unexpected response: {result}")
            return {
                'success': False,
                'error': result,
                'data': {}
            }
            
    except Exception as e:
        logger_polymarket.error(f"derive_api_key error: {e} line: {e.__traceback__.tb_lineno}")
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
        filename (str): Filename to save to
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
        
        logger_polymarket.info(f"Credentials saved to {filename}")
        return True
    except Exception as e:
        logger_polymarket.error(f"save_api_credentials error: {e}")
        return False


def main():
    import getpass
    
    print("\n=== Polymarket API Key Derivation ===\n")
    print("Use this tool to recover your existing API key if you've lost it.\n")
    
    private_key = getpass.getpass("Enter your Polygon private key (will not be displayed): ").strip()
    
    if not private_key:
        print("Error: Private key cannot be empty")
        return
    
    nonce_str = input("Enter the nonce used when creating the API key (default: 0): ").strip()
    nonce = int(nonce_str) if nonce_str else 0
    
    print(f"\nDeriving API key with nonce={nonce}...")
    result = derive_api_key(private_key, nonce)
    
    if result['success']:
        credentials = result['data']
        
        print("\n" + "="*50)
        print("API KEY DERIVED SUCCESSFULLY!")
        print("="*50)
        print(f"Address:    {credentials['address']}")
        print(f"API Key:    {credentials['key']}")
        print(f"Secret:     {credentials['secret']}")
        print(f"Passphrase: {credentials['passphrase']}")
        print("="*50)
        
        save_option = input("\nWould you like to save these credentials to a file? (y/n): ").strip().lower()
        
        if save_option == 'y':
            filename = input("Enter filename (default: .polymarket_credentials): ").strip()
            if not filename:
                filename = ".polymarket_credentials"
            
            if save_api_credentials(credentials, filename):
                print(f"✓ Credentials saved securely to {filename}")
                print("⚠ IMPORTANT: Keep this file secure and never share it!")
            else:
                print("✗ Failed to save credentials")
        
        print("\nUpdate your test configuration with:")
        print(f"API_KEY = '{credentials['key']}'")
        print(f"SECRET_KEY = '{credentials['secret']}'")
        print(f"WALLET_ADDRESS = '{credentials['address']}'")
        print(f"PASSPHRASE = '{credentials['passphrase']}'")
    else:
        print(f"\n✗ Error deriving API key: {result['error']}")


if __name__ == "__main__":
    main()
