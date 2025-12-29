import json
import requests
import time
from hashlib import sha256
import hmac
import base64

def parse_headers(api_key='', secret_key='', wallet_address='', passphrase=''):
    """
    Generates headers for Polymarket API requests with Builder API Key Authentication.
    
    Args:
        api_key (str): Builder API key (POLY_BUILDER_API_KEY)
        secret_key (str): Secret key for HMAC signature (POLY_BUILDER_SECRET)
        wallet_address (str): Polygon wallet address (POLY_ADDRESS)
        passphrase (str): Builder API passphrase (POLY_BUILDER_PASSPHRASE)
        
    Returns:
        dict: Headers for HTTP request
    """
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    timestamp = str(int(time.time() * 1000))
    
    if wallet_address:
        headers['POLY_ADDRESS'] = wallet_address
    
    if api_key:
        headers['POLY_BUILDER_API_KEY'] = api_key
    
    if passphrase:
        headers['POLY_BUILDER_PASSPHRASE'] = passphrase
    
    if secret_key and timestamp:
        headers['POLY_BUILDER_TIMESTAMP'] = timestamp
        headers['POLY_BUILDER_SIGNATURE'] = get_signature(secret_key, timestamp)
    
    return headers

def get_signature(secret_key, timestamp):
    """
    Generates a signature for Polymarket API requests using HMAC-SHA256.
    
    Args:
        secret_key (str): Base64-encoded secret key for HMAC
        timestamp (str): Timestamp for the request (in milliseconds)
        
    Returns:
        str: Hex-encoded HMAC signature
    """
    try:
        secret_bytes = base64.b64decode(secret_key)
    except Exception:
        secret_bytes = secret_key.encode('utf-8')
    
    message = timestamp.encode('utf-8')
    signature = hmac.new(secret_bytes, message, digestmod=sha256).hexdigest()
    return signature

def send_request(method, url, params_or_data=None, headers=None, timeout=30):
    """
    Sends an HTTP request to the Polymarket API.
    
    Args:
        method (str): HTTP method (GET, POST, DELETE, etc.)
        url (str): Full URL for the request
        params_or_data (dict): Query parameters for GET or body for POST/DELETE
        headers (dict): HTTP headers for the request
        timeout (int): Request timeout in seconds
        
    Returns:
        str: Response text from the API (JSON or error JSON)
    """
    if headers is None:
        headers = {}
    
    try:
        if method.upper() == 'GET':
            response = requests.request(
                method,
                url,
                params=params_or_data,
                headers=headers,
                timeout=timeout
            )
        elif method.upper() in ['POST', 'PUT']:
            response = requests.request(
                method,
                url,
                json=params_or_data,
                headers=headers,
                timeout=timeout
            )
        elif method.upper() == 'DELETE':
            response = requests.request(
                method,
                url,
                json=params_or_data if params_or_data else {},
                headers=headers,
                timeout=timeout
            )
        else:
            response = requests.request(
                method,
                url,
                params=params_or_data,
                headers=headers,
                timeout=timeout
            )
        
        if response.status_code >= 400:
            return json.dumps({'error': f'HTTP {response.status_code}: {response.text}'})
        
        return response.text
    except requests.exceptions.RequestException as e:
        return json.dumps({'error': str(e)})
    except Exception as e:
        return json.dumps({'error': str(e)})
