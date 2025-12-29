import time
import json
import redis
from logger import logger_polymarket
# from utils import calculate_gap_hours, get_candle_data_info, convert_order_status, get_precision_from_real_number
from utils import send_request, parse_headers



from py_clob_client.client import ClobClient
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.clob_types import ApiCreds as CliApiCreds

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

CLOB_API_URL = "https://clob.polymarket.com"
DATA_API_URL = "https://data-api.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Helper

FILLED_LIST_STATUS = ["full_fill", "full-fill", "FILLED", "closed", "filled", "fills","finished", "finish", "Filled", "done"]
PARTITAL_FILLED_LIST_STATUS = ['partial_fill', 'partially_filled', 'PARTIALLY_FILLED', 
                               'partial', 'PARTIAL', 'partial-filled', 'PartiallyFilled','part_filled']
NEW_LIST_STATUS = ['open', 'OPEN', "new", "NEW","PENDING", "pending", 'live', 'created', 'submitted','canceling', "wait", "watch", "PENDING_NEW"]
CANCELED_LIST_STATUS = ["cancelled", "CANCELLED", "cancel", "CANCEL", "canceled", "CANCELED",'canceled', 
                        'partial-canceled',"Cancelled", "part_canceled"]


def get_candle_data_info(symbol_redis, exchange_name, r, interval = '1h'):
    """
    A function that retrieves candle data information from the Redis cache or the exchange API.

    Args:
        symbol_redis (str): The symbol of the candle data in Redis.
        exchange_name (str): The name of the exchange.
        r (Redis): The Redis client.
        interval (str, optional): The time interval for the candle data. Defaults to '1h'.

    Returns:
        dict: A dictionary containing the candle data information.

    """
    now = int(time.time()*1000)
    if r.exists(f'{symbol_redis}_{exchange_name}_candle_{interval}') < 1:
        print(f'{symbol_redis}_{exchange_name}_candle_{interval} not exist')
        return None
    candles = json.loads(r.get(f'{symbol_redis}_{exchange_name}_candle_{interval}'))
    ts = float(candles['ts'])
    if now - ts >= 60000:
        print(f'{symbol_redis}_{exchange_name}_candle_{interval} delay_time_need_get_candle_api_intead')
        return None
    return candles

def convert_order_status(order_details_status):
    """
    Converts an order status from a specific exchange's format to a standardized format.

    Args:
        order_details_status (str): The order status from the exchange.

    Returns:
        str: The standardized order status. It can be one of the following: "FILLED", "PARTIALLY_FILLED", "NEW", "CANCELED", or "UNKNOWN".
    """
    order_status = None
    if order_details_status in FILLED_LIST_STATUS:
        order_status = ORDER_FILLED
    elif order_details_status in PARTITAL_FILLED_LIST_STATUS:
        order_status = ORDER_PARTIALLY_FILLED
    elif order_details_status in NEW_LIST_STATUS:
        order_status = ORDER_NEW
    elif order_details_status in CANCELED_LIST_STATUS:
        order_status = ORDER_CANCELLED
    else:
        order_status = ORDER_UNKNOWN
    return order_status

def get_precision_from_real_number(number):
    """
    Calculates the precision of a given real number.

    Args:
        number (float): The real number for which to calculate the precision.

    Returns:
        int: The precision of the given real number. If the number is an integer, the function returns the negative of the number
          of digits in the integer part. Otherwise, it returns the exponent of the number.
    """
    number = float(number)
    #print("al", number)
    if number % 1 ==0:
        count =0
        while number > 1:
            number =number //10
            #print(number)
            count +=1
        return -count
    precision = find_exp(number)
    #print("precission", precision)
    return precision


def calculate_gap_hours(ts1, ts2):
    """
    Calculate the gap in hours between two timestamps.

    Args:
        ts1 (int): The first timestamp in milliseconds.
        ts2 (int): The second timestamp in milliseconds.

    Returns:
        int: The gap in hours between the two timestamps.

    This function calculates the difference in seconds between two timestamps,
    converts it to hours, and rounds up to the nearest whole number. The input
    timestamps are assumed to be in milliseconds and are converted to seconds
    before the calculation. The function returns the gap in hours between the
    two timestamps.
    """
    # Convert milliseconds to seconds
    ts1_seconds = ts1
    ts2_seconds = ts2
    if len(str(int(ts1))) == 13:
        ts1_seconds = int(ts1 / 1000)
    if len(str(int(ts2))) == 13:
        ts2_seconds = int(ts2 / 1000)

    # Calculate the difference in seconds
    difference_seconds = abs(ts2_seconds - ts1_seconds)

    # Convert the difference to hours
    difference_hours = ceil(difference_seconds / 3600.0)

    return difference_hours


class PolymarketPrivate:
    """
    Class for interacting with the Polymarket CLOB API.
    """
    def __init__(self, api_key='', secret_key='', wallet_address='', passphrase='', private_key='', proxy_wallet=''):
        """
        Initializes a new instance of the `PolymarketPrivate` class.
        
        Args:
            api_key (str): Polymarket API key (POLY_API_KEY)
            secret_key (str): Secret key for HMAC signature (POLY_SECRET)
            wallet_address (str): Polygon wallet address (POLY_ADDRESS)
            passphrase (str): API key passphrase (POLY_PASSPHRASE)
            private_key (str): Polygon wallet private key (for signing orders)
        """
        self.api_key = api_key
        self.secret_key = secret_key
        self.wallet_address = wallet_address
        self.passphrase = passphrase
        self.private_key = private_key
        self.order_dict = {}
        self.proxy_wallet = proxy_wallet

    def _auth_headers(self, method, path, body=None):
        headers, serialized_body = parse_headers(
            method=method,
            request_path=path,
            body=body,
            api_key=self.api_key,
            secret_key=self.secret_key,
            wallet_address=self.wallet_address,
            passphrase=self.passphrase
        )
        return headers, serialized_body

    def _get_token_ids_from_market(self, market_id):
        """
        Helper method to fetch token IDs for a market from Gamma API.
        
        Args:
            market_id (str): Market ID
            
        Returns:
            tuple: (conditionId, [token_ids]) or (None, []) if not found
        """
        try:
            market_info = self.get_market_info(market_id)
            if market_info.get('data'):
                market_data = market_info['data']
                condition_id = market_data.get('conditionId', '')
                clobTokenIds_str = market_data.get('clobTokenIds', '[]')
                
                # Parse clobTokenIds if it's a string
                try:
                    if isinstance(clobTokenIds_str, str):
                        token_ids = json.loads(clobTokenIds_str)
                    else:
                        token_ids = clobTokenIds_str if isinstance(clobTokenIds_str, list) else []
                except (json.JSONDecodeError, TypeError):
                    token_ids = []
                
                return condition_id, token_ids
        except Exception as e:
            logger_polymarket.error(f"_get_token_ids_from_market error {e}")
        
        return None, []

    def get_trades(self, maker_address='', taker_address='', market='', before='', after=''):
        """
        Get trades from CLOB API.
        Reference: https://docs.polymarket.com/developers/CLOB/trades/trades
        
        Args:
            maker_address (str): Address to filter trades
            taker_address (str): Address to filter trades
            market (str): Market ID to filter trades
            before (str): Unix timestamp for pagination
            after (str): Unix timestamp for pagination
            
        Returns:
            dict: Response containing trades list or error
        """
        try:
            path = '/trades'
            method = "GET"
            
            params_map = {}
            if market:
                params_map['market'] = market
            if before:
                params_map['before'] = before
            if after:
                params_map['after'] = after
            
            url = f"{CLOB_API_URL}{path}"
            headers, _ = self._auth_headers(method, path)
            
            result = send_request(method, url, params_map, headers=headers)
            result = json.loads(result) if isinstance(result, str) else result
            
            if isinstance(result, dict) and 'data' in result:
                return {'data': result.get('data', [])}
            elif isinstance(result, list):
                return {'data': result}
            
            return {'data': []}
        except Exception as e:
            logger_polymarket.error(f"get_trades error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def get_scale(self, market_id):
        """
        Retrieves the price and quantity scales for the given market.
        """
        try:
            cache_key = f'polymarket_{market_id}_scale'
            scale_redis = r.get(cache_key)
            if scale_redis is not None:
                scale = json.loads(scale_redis)
                return int(scale["priceScale"]), int(scale["qtyScale"])
          
            book_data = self.get_order_book_full(market_id, outcome_index=0)
            
            price_scale = 2 # Default for Polymarket
            quantity_scale = 2 # Default
            if 'data' in book_data and book_data['data']:
                data = book_data['data']
                tick_size = data.get('tick_size')
                min_order_size = data.get('min_order_size')
                
                if tick_size:
                    price_scale = get_precision_from_real_number(float(tick_size))
                if min_order_size:
                    quantity_scale = float(min_order_size)
            
            scale = json.dumps({'priceScale': price_scale, 'qtyScale': quantity_scale})
            r.set(cache_key, scale, ex=3600) # Cache for 1 hour
            
            return price_scale, quantity_scale
        except Exception as e:
            logger_polymarket.error(f"get_scale error {e}")
            return 2, 2 

    def place_order(self, market_id, side, size, price, token_index=0, order_type='LIMIT', signature_type=None, funder=None):
        """
        Places an order on the Polymarket CLOB using py_clob_client.
        
        Args:
            market_id (str): Market ID
            side (str): Order side ('BUY' or 'SELL')
            size (str): Order size (in shares)
            price (str): Order price (0-1 for binary markets)
            token_index (int): Which token outcome to trade (0=first, 1=second). Default: 0
            order_type (str): Order type ('GTC', 'FOK', 'FAK'). Default: 'GTC'
            signature_type (int): Signature type (0 for EOA, 1 for POLY_PROXY, 2 for GNOSIS_SAFE). Default: None (EOA)
            funder (str): Funder address (Polymarket proxy address for funded accounts)
            type (str): Order strategy type ('LIMIT' or 'MARKET'). Default: 'LIMIT'
            
        Returns:
            dict: Response containing order details or error
        """
        try:
            if not self.private_key:
                return {'error': 'private_key is required to place orders', 'data': {}}
            
            price_scale, min_size = self.get_scale(market_id)
            if price_scale:
                price = round(float(price), price_scale)

            condition_id, token_ids = self._get_token_ids_from_market(market_id)
            if not token_ids or len(token_ids) <= token_index:
                return {'error': f'token_index {token_index} not found in market {market_id}. Available tokens: {len(token_ids)}', 'data': {}}
            
            token_id = token_ids[token_index]
            side_enum = BUY if side.upper() == 'BUY' else SELL
            funder = self.proxy_wallet if signature_type == 1  or signature_type == 2 else funder
            client_kwargs = {
                'host': CLOB_API_URL,
                'key': self.private_key,
                'chain_id': 137
            }
            if signature_type is not None:
                client_kwargs['signature_type'] = int(signature_type)
                if funder:
                    client_kwargs['funder'] = funder
                elif self.wallet_address:
                    client_kwargs['funder'] = self.wallet_address

            if (signature_type == 1 or signature_type == 2) and 'funder' not in client_kwargs:
                 client_kwargs['funder'] = self.wallet_address

            client = ClobClient(**client_kwargs)
            

            if self.api_key and self.secret_key and self.passphrase:
                api_creds = CliApiCreds(
                    api_key=self.api_key,
                    api_secret=self.secret_key,
                    api_passphrase=self.passphrase
                )
                client.set_api_creds(api_creds)
            else:
                client.set_api_creds(client.create_or_derive_api_creds())
            # client.set_api_creds(client.create_or_derive_api_creds())
            
            if order_type.upper() == 'MARKET' and side.upper() == 'BUY':
                size = float(size) * float(price)
                price = 1

            # if min_size and float(size) < min_size:
            #      return {'error': f'Size {size} is less than minimum order size {min_size}', 'data': {}}

            order_args = OrderArgs(
                token_id=token_id,
                price=float(price),
                size=float(size),
                side=side_enum
            )
            signed_order = client.create_order(order_args)
            
            # if type.upper() == 'MARKET':
            #     order_type_enum = OrderType.FOK
            # elif order_type.upper() == 'FOK':
            #     order_type_enum = OrderType.FOK
            # elif order_type.upper() == 'FAK':
            #     order_type_enum = OrderType.FAK
            # else:
            #     order_type_enum = OrderType.GTC

            if order_type.upper() == 'MARKET':
                order_type_enum = OrderType.FOK
            else:
                order_type_enum = OrderType.GTC


            order_result = client.post_order(signed_order, order_type_enum)
            if order_result:
                return {'data': order_result}
            else:
                return {'error': 'Failed to place order', 'data': {}}
        except Exception as e:
            logger_polymarket.error(f"place_order error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def place_order_v2(self, market_id, side, size, price, token_index=0, order_type='LIMIT', signature_type=None, funder=None):
        """
        Places an order on the Polymarket CLOB using L2 Authentication (API Key + Signature).
        Reference: https://docs.polymarket.com/developers/CLOB/authentication#python-2
        
        Args:
            market_id (str): Market ID
            side (str): Order side ('BUY' or 'SELL')
            size (str): Order size (in shares)
            price (str): Order price (0-1 for binary markets)
            token_index (int): Which token outcome to trade (0=first, 1=second). Default: 0
            order_type (str): Order type ('GTC', 'FOK', 'FAK'). Default: 'GTC'
            signature_type (int): Signature type (0 for EOA, 1 for POLY_PROXY, 2 for GNOSIS_SAFE). Default: None (EOA)
            funder (str): Funder address (Polymarket proxy address for funded accounts)
            type (str): Order strategy type ('LIMIT' or 'MARKET'). Default: 'LIMIT'
            
        Returns:
            dict: Response containing order details or error
        """
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.order_builder.constants import BUY, SELL
            from py_clob_client.clob_types import ApiCreds, OrderArgs, PartialCreateOrderOptions, OrderType
            
            if not self.private_key:
                return {'error': 'private_key is required to place orders', 'data': {}}
            
            if not self.api_key or not self.secret_key or not self.passphrase:
                return {'error': 'API credentials (api_key, secret_key, passphrase) are required for L2 auth', 'data': {}}
            
            price_scale, min_size = self.get_scale(market_id)
            if price_scale:
                price = round(float(price), price_scale)

            condition_id, token_ids = self._get_token_ids_from_market(market_id)
            if not token_ids or len(token_ids) <= token_index:
                return {'error': f'token_index {token_index} not found in market {market_id}. Available tokens: {len(token_ids)}', 'data': {}}
            
            token_id = token_ids[token_index]
            side_enum = BUY if side.upper() == 'BUY' else SELL
            
            # Determine funder address
            if signature_type == 1 or signature_type == 2:
                if not funder and self.proxy_wallet:
                    funder = self.proxy_wallet
                elif not funder and self.wallet_address:
                    funder = self.wallet_address
            
            # Create API credentials for L2 auth
            api_creds = ApiCreds(
                api_key=self.api_key,
                api_secret=self.secret_key,
                api_passphrase=self.passphrase
            )
            
            # Initialize ClobClient with credentials passed directly
            client_kwargs = {
                'host': CLOB_API_URL,
                'chain_id': 137,
                'key': self.private_key,
                'creds': api_creds
            }
            
            if signature_type is not None:
                client_kwargs['signature_type'] = int(signature_type)
            
            if funder:
                client_kwargs['funder'] = funder
            
            # print(f"DEBUG: Initializing ClobClient L2 with kwargs: {client_kwargs}")
            client = ClobClient(**client_kwargs)
            
            # Create order arguments and options
            if order_type.upper() == 'MARKET' and side.upper() == 'BUY':
                size = float(size) * float(price)
                price = 1

            # if min_size and float(size) < min_size:
            #      return {'error': f'Size {size} is less than minimum order size {min_size}', 'data': {}}

            order_args = OrderArgs(
                token_id=token_id,
                price=float(price),
                size=float(size),
                side=side_enum
            )
            
            order_options = PartialCreateOrderOptions(
                tick_size='0.01',
                neg_risk=False
            )
            
            # Create and sign order
            signed_order = client.create_order(order_args, order_options)
            
            # Map order type for post_order
            # order_type_upper = order_type.upper()
            # if order_type_upper == 'FOK':
            #     order_type_enum = OrderType.FOK
            # if order_type_upper == 'FAK':
            #     order_type_enum = OrderType.FAK
            if order_type.upper() == 'MARKET':
                order_type_enum = OrderType.FOK
            else:
                order_type_enum = OrderType.GTC
            
            order_result = client.post_order(signed_order, order_type_enum)
            
            if order_result:
                return {'data': order_result}
            else:
                return {'error': 'Failed to place order', 'data': {}}
        except Exception as e:
            logger_polymarket.error(f"place_order_v2 error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def cancel_order(self, order_id):
        """
        Cancels a single OPEN order by ID using py_clob_client.
        Note: Only cancels open/pending orders, not filled/matched orders that are already executed.
        Reference: https://docs.polymarket.com/developers/CLOB/orders/cancel-orders
        
        Args:
            order_id (str): Order ID or order hash to cancel (e.g., '0x422e1a21...')
            
        Returns:
            dict: Response with canceled and not_canceled order information
        """
        try:
            from py_clob_client.client import ClobClient
            
            if not self.private_key:
                return {'error': 'private_key is required to cancel orders', 'data': {}}
            
            client_kwargs = {
                'host': CLOB_API_URL,
                'key': self.private_key,
                'chain_id': 137
            }
            
            client = ClobClient(**client_kwargs)
            
            if self.api_key and self.secret_key and self.passphrase:
                from py_clob_client.clob_types import ApiCreds as CliApiCreds
                api_creds = CliApiCreds(
                    api_key=self.api_key,
                    api_secret=self.secret_key,
                    api_passphrase=self.passphrase
                )
                client.set_api_creds(api_creds)
            else:
                client.set_api_creds(client.create_or_derive_api_creds())
            
            result = client.cancel(order_id=order_id)
            
            return {'data': result if result else {'canceled': [], 'not_canceled': {}}}
        except ImportError:
            logger_polymarket.error("py_clob_client not installed")
            return {'error': 'py_clob_client library not available', 'data': {}}
        except Exception as e:
            logger_polymarket.error(f"cancel_order error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def get_order_details(self, order_id):
        """
        Retrieves the details of an order with the given order ID.
        Reference: https://docs.polymarket.com/developers/CLOB/orders/get-order
        
        Args:
            order_id (str): Order ID or order hash (e.g., '0x422e1a21...')
            
        Returns:
            dict: Order details or error
        """
        try:
            from py_clob_client.client import ClobClient
            
            if not self.private_key:
                return {'error': 'private_key is required to get order details', 'data': {}}
            
            client_kwargs = {
                'host': CLOB_API_URL,
                'key': self.private_key,
                'chain_id': 137
            }
            
            client = ClobClient(**client_kwargs)
            
            if self.api_key and self.secret_key and self.passphrase:
                from py_clob_client.clob_types import ApiCreds as CliApiCreds
                api_creds = CliApiCreds(
                    api_key=self.api_key,
                    api_secret=self.secret_key,
                    api_passphrase=self.passphrase
                )
                client.set_api_creds(api_creds)
            else:
                client.set_api_creds(client.create_or_derive_api_creds())
            
            order_info = client.get_order(order_id)
            
            if order_info:
                data = {
                    'clientOrderId': order_info.get('id', ''),
                    'orderId': order_info.get('id', ''),
                    'status': convert_order_status(order_info.get('status', 'UNKNOWN')),
                    'side': order_info.get('side', ''),
                    'price': order_info.get('price', '0'),
                    'size': order_info.get('original_size', order_info.get('size', '0')),
                    'quantity': order_info.get('original_size', order_info.get('size', '0')),
                    'fillQuantity': order_info.get('size_matched', '0'),
                    'market': order_info.get('market', ''),
                    'outcome': order_info.get('outcome', ''),
                    'type': order_info.get('order_type', order_info.get('type', '')),
                    'orderCreateTime': order_info.get('created_at', ''),
                    'orderUpdateTime': order_info.get('updated_at', order_info.get('created_at', ''))
                }
                return {'data': data}
        except ImportError:
            logger_polymarket.error("py_clob_client not installed")
            return {'error': 'py_clob_client library not available', 'data': {}}
        except Exception as e:
            logger_polymarket.error(f"get_order_details error {e} line: {e.__traceback__.tb_lineno}")
        
        return {'data': None}

    def get_open_orders(self, market_id=''):
        """
        Retrieves the open orders for the user using py_clob_client.
        
        Args:
            market_id (str): Optional market ID to filter orders (not supported by py_clob_client, use client-side filtering)
            
        Returns:
            dict: List of open orders or error
        """
        try:
            from py_clob_client.client import ClobClient
            
            if not self.private_key:
                return {'error': 'private_key is required to get open orders', 'data': []}
            
            client_kwargs = {
                'host': CLOB_API_URL,
                'key': self.private_key,
                'chain_id': 137
            }
            
            client = ClobClient(**client_kwargs)
            
            if self.api_key and self.secret_key and self.passphrase:
                from py_clob_client.clob_types import ApiCreds as CliApiCreds
                api_creds = CliApiCreds(
                    api_key=self.api_key,
                    api_secret=self.secret_key,
                    api_passphrase=self.passphrase
                )
                client.set_api_creds(api_creds)
            else:
                client.set_api_creds(client.create_or_derive_api_creds())
            
            orders = client.get_orders()
            
            if isinstance(orders, list):
                filtered_orders = []
                for order in orders:
                    if market_id and order.get('market') != market_id:
                        continue
                    order['status'] = convert_order_status(order.get('status', 'UNKNOWN'))
                    filtered_orders.append(order)
                return {'data': filtered_orders}
            else:
                return {'data': []}
        except ImportError:
            logger_polymarket.error("py_clob_client not installed")
            return {'error': 'py_clob_client library not available', 'data': []}
        except Exception as e:
            logger_polymarket.error(f"get_open_orders error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def get_account_balance(self, signature_type=None, funder=None):
        """
        Retrieves the account balance/assets for the user using get_balance_allowance.
        
        Args:
            signature_type (int): Signature type (0 for EOA, 1 for POLY_PROXY, 2 for GNOSIS_SAFE)
            funder (str): Funder address (Polymarket proxy address)
        
        Returns:
            dict: Account balance information in format:
                {
                    'data': {
                        'total': '2020766',
                        'available': '2020766',
                        'allowance': '115792089237316195423570985008687907853269984665640564039457584007913128639936',
                        'locked': 0.0
                    }
                }
        """
        try:
            cache_key = f'polymarket_{self.wallet_address}_balance_{signature_type or 0}'
            cached_data = r.get(cache_key)
            if cached_data is not None:
                return {'data': json.loads(cached_data)}
            
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            
            if not self.private_key:
                return {'error': 'private_key is required to get balance', 'data': {}}
            
            # Use get_balance_allowance to fetch balance
            balance_result = self.get_balance_allowance(
                params=None,
                signature_type=signature_type,
                funder=funder
            )
                        
            if 'error' in balance_result:
                logger_polymarket.error(f"get_account_balance - get_balance_allowance failed: {balance_result.get('error')}")
                return balance_result
            
            result = balance_result.get('data', {})
            
            if result and isinstance(result, dict):
                balance = result.get('balance', '0')
                allowances = result.get('allowances', {})
                
                # Get the first allowance value or '0' if no allowances
                first_allowance = list(allowances.values())[0] if allowances else '0'
                
                account_balance = {
                    'total': str(float(balance) / 1e6),
                    'available': str(float(balance) / 1e6),
                    'allowance': str(first_allowance),
                    'locked': 0.0
                }
                r.set(cache_key, json.dumps(account_balance), ex=5)
                return {'data': account_balance}
            
            return {'error': 'No balance data returned', 'data': {}}
        except ImportError:
            logger_polymarket.error("py_clob_client not installed")
            return {'error': 'py_clob_client library not available', 'data': {}}
        except Exception as e:
            logger_polymarket.error(f"get_account_balance error {e} line: {e.__traceback__.tb_lineno if hasattr(e, '__traceback__') else 'N/A'}")
            return {'error': str(e), 'data': {}}

    def get_balance_allowance(self, params=None, signature_type=None, funder=None):
        """
        Fetches the balance & allowance for a user.
        
        Args:
            params (BalanceAllowanceParams): Parameters for the request
            signature_type (int): Signature type (0 for EOA, 1 for POLY_PROXY, 2 for GNOSIS_SAFE)
            funder (str): Funder address (Polymarket proxy address)
            
        Returns:
            dict: Balance and allowance data
        """
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            
            if not self.private_key:
                return {'error': 'private_key is required', 'data': {}}
            
            client_kwargs = {
                'host': CLOB_API_URL,
                'key': self.private_key,
                'chain_id': 137
            }
            
            if signature_type is not None:
                client_kwargs['signature_type'] = int(signature_type)
            if funder:
                client_kwargs['funder'] = funder
            
            client = ClobClient(**client_kwargs)
            
            if self.api_key and self.secret_key and self.passphrase:
                from py_clob_client.clob_types import ApiCreds as CliApiCreds
                api_creds = CliApiCreds(
                    api_key=self.api_key,
                    api_secret=self.secret_key,
                    api_passphrase=self.passphrase
                )
                client.set_api_creds(api_creds)
            else:
                client.set_api_creds(client.create_or_derive_api_creds())
            
            if params is None:
                params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                
            result = client.get_balance_allowance(params)
            return {'data': result}
            
        except Exception as e:
            logger_polymarket.error(f"get_balance_allowance error {e}")
            return {'error': str(e), 'data': {}}

    def get_account_assets(self, asset_type=''):
        """
        Retrieves detailed account assets.
        
        Args:
            asset_type (str): Optional asset type filter
            
        Returns:
            dict: Asset information or error
        """
        try:
            cache_key = f'polymarket_{self.wallet_address}_assets'
            cached_data = r.get(cache_key)
            if cached_data is not None:
                cached_dict = json.loads(cached_data)
                if asset_type and asset_type in cached_dict:
                    return {'data': cached_dict[asset_type]}
                return {'data': cached_dict}
            
            balance_data = self.get_account_balance()
            if 'data' in balance_data:
                assets_dict = {
                    'balance': balance_data['data']
                }
                r.set(cache_key, json.dumps(assets_dict), ex=5)
                return {'data': assets_dict}
            
            return {'error': balance_data.get('error', 'Failed to get assets'), 'data': {}}
        except Exception as e:
            logger_polymarket.error(f"get_account_assets error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def delete_full_filled_order(self, order_id):
        """
        Deletes a full-filled order from the order dictionary.
        
        Args:
            order_id (str): Order ID to delete
        """
        if order_id in self.order_dict:
            self.order_dict.pop(order_id)

    def place_batch_orders(self, orders_list):
        """
        Places multiple orders in a single batch (up to 15 orders).
        Reference: https://docs.polymarket.com/developers/CLOB/orders/create-order-batch
        
        Args:
            orders_list (list): List of order dictionaries. Each order dict should have:
                - 'order': Signed order object with salt, maker, signer, taker, tokenId, makerAmount, 
                  takerAmount, expiration, nonce, feeRateBps, side, signatureType, signature
                - 'orderType': Order type ('GTC', 'FOK', 'FAK', 'GTD')
                - 'owner': API key of order owner
            
        Returns:
            dict: Response containing placed orders or error
        """
        try:
            path = '/orders'
            method = "POST"
            
            payload = orders_list if isinstance(orders_list, list) else {'orders': orders_list}
            
            url = f"{CLOB_API_URL}{path}"
            headers, serialized_body = self._auth_headers(method, path, payload)
            
            result = send_request(
                method,
                url,
                payload,
                headers=headers,
                serialized_body=serialized_body
            )
            result = json.loads(result) if isinstance(result, str) else result
            
            return {'data': result} if result else {'error': 'Failed to place batch orders', 'data': []}
        except Exception as e:
            logger_polymarket.error(f"place_batch_orders error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def cancel_orders(self):
        """
        Cancels ALL OPEN orders for the user (orders waiting on the order book).
        Note: Only cancels open/pending orders, not filled/matched orders that are already executed.
        Reference: https://docs.polymarket.com/developers/CLOB/orders/cancel-orders
        
        Returns:
            dict: Response with canceled and not_canceled order information.
                  - canceled: list of order IDs that were successfully canceled
                  - not_canceled: dict of order IDs that failed to cancel with reasons
        """
        try:
            from py_clob_client.client import ClobClient
            
            if not self.private_key:
                return {'error': 'private_key is required to cancel orders', 'data': {}}
            
            client_kwargs = {
                'host': CLOB_API_URL,
                'key': self.private_key,
                'chain_id': 137
            }
            
            client = ClobClient(**client_kwargs)
            
            if self.api_key and self.secret_key and self.passphrase:
                from py_clob_client.clob_types import ApiCreds as CliApiCreds
                api_creds = CliApiCreds(
                    api_key=self.api_key,
                    api_secret=self.secret_key,
                    api_passphrase=self.passphrase
                )
                client.set_api_creds(api_creds)
            else:
                client.set_api_creds(client.create_or_derive_api_creds())
            
            result = client.cancel_all()
            
            return {'data': result if result else {'canceled': [], 'not_canceled': {}}}
        except ImportError:
            logger_polymarket.error("py_clob_client not installed")
            return {'error': 'py_clob_client library not available', 'data': {}}
        except Exception as e:
            logger_polymarket.error(f"cancel_all error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def get_user_trades(self, limit=100, before=''):
        """
        Get all trades for the current user.
        
        Args:
            limit (int): Number of trades to retrieve
            before (str): Unix timestamp for pagination
            
        Returns:
            dict: List of trades or error
        """
        try:
            return self.get_trades(
                maker_address=self.wallet_address,
                before=before
            )
        except Exception as e:
            logger_polymarket.error(f"get_user_trades error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}



##############################################################################################################
## Get market data   
##############################################################################################################
    
    def get_topics(self, limit=100):
        """
        Retrieves all available topics/tags from Polymarket using Gamma API (public endpoint).
        Features: no auth required, 60sec cache, returns tag metadata.
        Use: discover market categories, topic filtering, market classification.
        
        Args:
            limit (int): Max tags to retrieve (default 100)
            
        Returns:
            dict: List of all topics/tags or error
        """
        try:
            cache_key = f'polymarket_topics_{limit}'
            cached_data = r.get(cache_key)
            if cached_data is not None:
                return {'data': json.loads(cached_data)}
            
            path = '/tags'
            method = "GET"
            params_map = {'limit': limit}
            
            url = f"{GAMMA_API_URL}{path}"
            
            result = send_request(method, url, params_map, headers={})
            result = json.loads(result) if isinstance(result, str) else result
            
            if isinstance(result, list):
                r.set(cache_key, json.dumps(result), ex=60)
                return {'data': result}
            
            return {'error': 'No topics found', 'data': []}
        except Exception as e:
            logger_polymarket.error(f"get_topics error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def get_rates_by_topic(self, tag_id=None):
        """
        Retrieves markets and rates for a specific topic/tag using Gamma API (public endpoint).
        Features: aggregated data per topic, volume/liquidity metrics, 30sec cache.
        Use: topic-based portfolio tracking, category analysis, sentiment per topic.
        
        Args:
            tag_id (int): Specific tag ID to filter (optional, gets all if None)
            
        Returns:
            dict: Markets with prices, volumes, liquidity per topic or error
        """
        try:
            if tag_id is None:
                cache_key = 'polymarket_all_topic_rates'
            else:
                cache_key = f'polymarket_topic_rates_{tag_id}'
            
            cached_data = r.get(cache_key)
            if cached_data is not None:
                return {'data': json.loads(cached_data)}
            
            path = '/markets'
            method = "GET"
            params_map = {'closed': False, 'limit': 500}
            
            if tag_id:
                params_map['tag_id'] = tag_id
            
            url = f"{GAMMA_API_URL}{path}"
            
            result = send_request(method, url, params_map, headers={})
            result = json.loads(result) if isinstance(result, str) else result
            
            if isinstance(result, list):
                markets_data = []
                for market in result:
                    try:
                        outcome_prices = market.get('outcomePrices', [0])
                        if isinstance(outcome_prices, str):
                            import ast
                            outcome_prices = ast.literal_eval(outcome_prices)
                        price = float(outcome_prices[0]) if outcome_prices else 0
                    except (ValueError, IndexError, TypeError):
                        price = 0
                    
                    market_data = {
                        'id': market.get('id', ''),
                        'question': market.get('question', ''),
                        'price': price,
                        'volume': float(market.get('volume', 0)) if market.get('volume') else 0,
                        'volume_24h': float(market.get('volume24hr', 0)) if market.get('volume24hr') else 0,
                        'volume_1w': float(market.get('volume1wk', 0)) if market.get('volume1wk') else 0,
                        'liquidity': float(market.get('liquidity', 0)) if market.get('liquidity') else 0
                    }
                    markets_data.append(market_data)
                
                r.set(cache_key, json.dumps(markets_data), ex=30)
                return {'data': markets_data}
            
            return {'error': 'No markets found', 'data': []}
        except Exception as e:
            logger_polymarket.error(f"get_rates_by_topic error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def get_topic_statistics(self, tag_id=None):
        """
        Comprehensive topic statistics using Gamma API (public endpoint).
        Features: aggregate metrics per tag/event, volume/liquidity rankings, 60sec cache.
        Use: topic performance analysis, category comparison, market health assessment.
        
        Args:
            tag_id (int): Specific tag ID (optional, gets all events if None)
            
        Returns:
            dict: Topic statistics with volume, liquidity, market count or error
        """
        try:
            if tag_id is None:
                cache_key = 'polymarket_all_topic_stats'
            else:
                cache_key = f'polymarket_topic_stats_{tag_id}'
            
            cached_data = r.get(cache_key)
            if cached_data is not None:
                return {'data': json.loads(cached_data)}
            
            path = '/events'
            method = "GET"
            params_map = {'closed': False, 'limit': 500}
            
            if tag_id:
                params_map['tag_id'] = tag_id
            
            url = f"{GAMMA_API_URL}{path}"
            
            result = send_request(method, url, params_map, headers={})
            result = json.loads(result) if isinstance(result, str) else result
            
            if isinstance(result, list):
                stats_list = []
                for event in result:
                    stat = {
                        'id': event.get('id', ''),
                        'title': event.get('title', ''),
                        'tag_id': event.get('tag_id', ''),
                        'volume': float(event.get('volume', 0)),
                        'volume_24h': float(event.get('volume24hr', 0)),
                        'volume_1w': float(event.get('volume1wk', 0)),
                        'liquidity': float(event.get('liquidity', 0)),
                        'market_count': len(event.get('markets', [])) if event.get('markets') else 0
                    }
                    stats_list.append(stat)
                
                sorted_stats = sorted(stats_list, key=lambda x: x['liquidity'], reverse=True)
                r.set(cache_key, json.dumps(sorted_stats), ex=60)
                return {'data': sorted_stats}
            
            return {'error': 'No events found', 'data': []}
        except Exception as e:
            logger_polymarket.error(f"get_topic_statistics error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def get_market_rates(self, market_ids=None):
        """
        Get current rates (prices) for specific markets. Features: batch price fetch, format normalization.
        Use: portfolio pricing, position valuation, rate monitoring.
        
        Args:
            market_ids (list): List of market IDs to get rates for
            
        Returns:
            dict: Market rates with prices or error
        """
        try:
            if not market_ids or len(market_ids) == 0:
                return {'error': 'No market IDs provided', 'data': []}
            
            rates = []
            for market_id in market_ids:
                market_info = self.get_market_info(market_id)
                if market_info.get('data'):
                    info = market_info['data']
                    rate_data = {
                        'market_id': market_id,
                        'price': float(info.get('lastPrice', 0)),
                        'price_yes': float(info.get('lastPriceYes', 0)) if info.get('lastPriceYes') else None,
                        'price_no': float(info.get('lastPriceNo', 0)) if info.get('lastPriceNo') else None,
                        'volume': float(info.get('volume', 0)),
                        'volume_24h': float(info.get('volume24h', 0)),
                        'timestamp': int(time.time() * 1000)
                    }
                    rates.append(rate_data)
            
            return {'data': rates} if rates else {'data': []}
        except Exception as e:
            logger_polymarket.error(f"get_market_rates error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    
    
    def get_market_info(self, market_id=''):
        """
        Retrieves market information from Gamma API.
        
        Args:
            market_id (str): Market ID or condition ID
            
        Returns:
            dict: Market information or error
        """
        try:
            path = '/markets'
            method = "GET"
            
            params_map = {}
            if market_id:
                params_map['id'] = market_id
            
            url = f"{GAMMA_API_URL}{path}"
            
            result = send_request(method, url, params_map, headers={})
            result = json.loads(result) if isinstance(result, str) else result
            
            if result:
                if isinstance(result, list) and len(result) > 0:
                    return {'data': result[0]}
                elif isinstance(result, dict):
                    return {'data': result}
            
            return {'data': {}}
        except Exception as e:
            logger_polymarket.error(f"get_market_info error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def get_orderbook(self, market_id, outcome_index=0, depth=50):
        """
        Retrieves the order book for a specific market from CLOB API.
        
        Args:
            market_id (str): Market ID
            outcome_index (int): Which outcome to fetch (0=first outcome, 1=second outcome). Default: 0
            depth (int): Number of levels per side to return. Default: 50
            
        Returns:
            dict: Order book data with bids/asks dicts, or None on error
        """
        try:
            cache_key = f'polymarket_{market_id}_outcome{outcome_index}_orderbook'
            cached_data = r.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
            
            condition_id, token_ids = self._get_token_ids_from_market(market_id)
                        
            if not token_ids or outcome_index >= len(token_ids):
                return None
            
            token_id = token_ids[outcome_index]
            params_map = {'token_id': token_id}
            
            url = f"{CLOB_API_URL}/book"
                        
            result = send_request("GET", url, params_map, headers={})
            result = json.loads(result) if isinstance(result, str) else result
            if isinstance(result, dict) and 'error' not in result:
                bids = result.get('bids', [])[:depth]
                asks = result.get('asks', [])[:depth]
                
                bids_dict = {str(item.get('price', 0)): str(item.get('size', 0)) for item in bids}
                asks_dict = {str(item.get('price', 0)): str(item.get('size', 0)) for item in asks}
                
                orderbook = {
                    'ts': int(time.time() * 1000),
                    'bids': bids_dict,
                    'asks': asks_dict,
                }
                
                r.set(cache_key, json.dumps(orderbook, separators=(",", ":")), ex=2)
                return orderbook
            
            return None
        except Exception as e:
            logger_polymarket.error(f"get_orderbook error {e} line: {e.__traceback__.tb_lineno}")
            return None

    def get_ticker(self, market_id=''):
        """
        Retrieves ticker information for a market.
        
        Args:
            market_id (str): Market ID
            
        Returns:
            dict: Ticker data with bidPr, askPr, bidSz, askSz, last, lastPr, ts, or None on error
        """
        try:
            cache_key = f'{market_id}_polymarket_ticker'
            cached_data = r.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
            
            market_info = self.get_market_info(market_id)
            orderbook = self.get_orderbook(market_id) if market_id else None
            
            if market_info.get('data'):
                market_data = market_info['data']
                
                best_bid = None
                best_ask = None
                bid_size = None
                ask_size = None
                mid_price = None
                
                if orderbook:
                    bids = orderbook.get('bids', {})
                    asks = orderbook.get('asks', {})
                    
                    if bids:
                        best_bid_price = max(float(p) for p in bids.keys() if p)
                        best_bid = str(best_bid_price)
                        bid_size = bids.get(best_bid)
                    
                    if asks:
                        best_ask_price = min(float(p) for p in asks.keys() if p)
                        best_ask = str(best_ask_price)
                        ask_size = asks.get(best_ask)
                    
                    if best_bid and best_ask:
                        mid_price = (float(best_bid) + float(best_ask)) / 2
                
                last_price = market_data.get('lastTradePrice') or market_data.get('lastPrice')
                if not last_price and mid_price:
                    last_price = mid_price
                
                tick = {
                    'bidPr': best_bid,
                    'askPr': best_ask,
                    'bidSz': bid_size,
                    'askSz': ask_size,
                    'last': last_price,
                    'lastPr': last_price,
                    'ts': int(time.time() * 1000),
                }
                
                r.set(cache_key, json.dumps(tick))
                return tick
            
            return None
        except Exception as e:
            logger_polymarket.error(f"get_ticker error {e} line: {e.__traceback__.tb_lineno}")
            return None

    def get_order_book_full(self, market_id, outcome_index=0):
        """
        Full orderbook with all bid/ask levels. Features: complete depth, mid-price calc, market metadata, 2sec cache.
        Use: market-making, price analysis, liquidity assessment.
        
        Args:
            market_id (str): Market ID
            outcome_index (int): Which outcome to fetch (0=first outcome, 1=second outcome). Default: 0
            
        Returns:
            dict: Full orderbook data with bids, asks, mid_price, market metadata or error
        """
        try:
            cache_key = f'polymarket_{market_id}_orderbook_full'
            cached_data = r.get(cache_key)
            if cached_data is not None:
                return {'data': json.loads(cached_data)}
            
            condition_id, token_ids = self._get_token_ids_from_market(market_id)
            
            if not token_ids or outcome_index >= len(token_ids):
                return {'error': f'No token ID found for market {market_id}', 'data': {}}
            
            token_id = token_ids[outcome_index]
            path = f'/book'
            method = "GET"
            params_map = {'token_id': token_id}
            
            url = f"{CLOB_API_URL}{path}"
            headers = parse_headers(
                api_key=self.api_key,
                secret_key=self.secret_key,
                wallet_address=self.wallet_address,
                passphrase=self.passphrase
            )
            
            result = send_request(method, url, params_map, headers=headers)
            result = json.loads(result) if isinstance(result, str) else result
            
            if result:
                orderbook = {
                    'market': result.get('market', market_id),
                    'asset_id': result.get('asset_id', ''),
                    'timestamp': result.get('timestamp', ''),
                    'hash': result.get('hash', ''),
                    'min_order_size': result.get('min_order_size', '0'),
                    'tick_size': result.get('tick_size', '0'),
                    'neg_risk': result.get('neg_risk', False),
                    'bids': result.get('bids', []),
                    'asks': result.get('asks', []),
                    'mid_price': self._calculate_mid_price(result.get('bids', []), result.get('asks', []))
                }
                r.set(cache_key, json.dumps(orderbook), ex=2)
                return {'data': orderbook}
            
            return {'error': 'No orderbook data', 'data': {}}
        except Exception as e:
            logger_polymarket.error(f"get_order_book_full error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def get_order_book_depth(self, market_id, depth=10):
        """
        Top N price levels orderbook. Features: configurable depth, spread calc, mid-price, reduced payload.
        Use: real-time monitoring, bot polling, lightweight updates.
        
        Args:
            market_id (str): Market ID (condition ID)
            depth (int): Number of levels per side to return (default: 10)
            
        Returns:
            dict: Top N bids/asks with spread and mid_price or error
        """
        try:
            orderbook = self.get_order_book_full(market_id)
            
            if orderbook.get('data'):
                data = orderbook['data']
                return {
                    'data': {
                        'market': data.get('market'),
                        'timestamp': data.get('timestamp'),
                        'bids': data.get('bids', [])[:depth],
                        'asks': data.get('asks', [])[:depth],
                        'mid_price': data.get('mid_price'),
                        'spread': self._calculate_spread(
                            data.get('bids', []),
                            data.get('asks', [])
                        )
                    }
                }
            
            return orderbook
        except Exception as e:
            logger_polymarket.error(f"get_order_book_depth error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}
        
    def get_multiple_orderbooks(self, market_ids):
        """
        Batch orderbook fetch by iterating over markets. Features: efficient bulk operation.
        Use: portfolio monitoring, multi-position updates, liquidity assessment.
        
        Args:
            market_ids (list): List of market IDs to fetch
            
        Returns:
            dict: Dictionary of orderbooks keyed by market_id or error
        """
        try:
            orderbooks = {}
            
            for market_id in market_ids:
                result = self.get_order_book_full(market_id)
                if result.get('data'):
                    orderbooks[market_id] = result['data']
            
            return {'data': orderbooks} if orderbooks else {'data': {}}
        except Exception as e:
            logger_polymarket.error(f"get_multiple_orderbooks error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}


    def get_market_prices(self, market_ids=None):
        """
        Current prices for multiple markets from Gamma API. Features: bulk query, price snapshot.
        Use: portfolio valuation, price monitoring, position tracking.
        
        Args:
            market_ids (list): List of market IDs to get prices for
            
        Returns:
            dict: Price data for queried markets or error
        """
        try:
            path = '/markets'
            method = "GET"
            
            params_map = {'limit': 100}
            
            url = f"{GAMMA_API_URL}{path}"
            
            result = send_request(method, url, params_map, headers={})
            result = json.loads(result) if isinstance(result, str) else result
            
            if isinstance(result, list):
                prices = []
                for market in result:
                    if market_ids and market.get('id') not in market_ids:
                        continue
                    price_data = {
                        'market_id': market.get('id', ''),
                        'price': float(market.get('lastPrice', 0)) if market.get('lastPrice') else 0,
                        'timestamp': int(time.time() * 1000)
                    }
                    prices.append(price_data)
                return {'data': prices}
            
            return {'data': []}
        except Exception as e:
            logger_polymarket.error(f"get_market_prices error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def get_market_spreads(self, market_ids=None):
        """
        Bid-ask spreads with percentage calculations. Features: liquidity metrics, spread_pct, multi-market support.
        Use: trading cost estimation, liquidity analysis, arbitrage detection.
        
        Args:
            market_ids (list): List of market IDs to get spreads for
            
        Returns:
            dict: bid, ask, spread, spread_pct for each market or error
        """
        try:
            spreads = {}
            
            if market_ids:
                for market_id in market_ids:
                    orderbook = self.get_order_book_full(market_id)
                    if orderbook.get('data'):
                        data = orderbook['data']
                        bids = data.get('bids', [])
                        asks = data.get('asks', [])
                        
                        bid_price = bids[0].get('price') if bids else None
                        ask_price = asks[0].get('price') if asks else None
                        
                        spread = self._calculate_spread(bids, asks)
                        spread_pct = 0
                        if spread and bid_price:
                            try:
                                spread_pct = float(spread) / float(bid_price) * 100
                            except (ValueError, ZeroDivisionError):
                                spread_pct = 0
                        
                        spreads[market_id] = {
                            'bid': bid_price,
                            'ask': ask_price,
                            'spread': spread,
                            'spread_pct': spread_pct,
                            'timestamp': data.get('timestamp')
                        }
            
            return {'data': spreads} if spreads else {'data': {}}
        except Exception as e:
            logger_polymarket.error(f"get_market_spreads error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def get_active_markets(self, status='active', sort_by='volume', limit=100):
        """
        Active markets from Gamma API with filtering & sorting. Features: status filter, multi-sort (volume/liquidity), 10sec cache.
        Use: market discovery, find high-volume pools, market scanning.
        
        Args:
            status (str): 'active', 'closed', 'resolved'
            sort_by (str): 'volume', 'liquidity', 'updated_at'
            limit (int): Max markets to return
            
        Returns:
            dict: Filtered/sorted market list or error
        """
        try:
            cache_key = f'polymarket_active_markets_{status}_{sort_by}_{limit}'
            cached_data = r.get(cache_key)
            if cached_data is not None:
                return {'data': json.loads(cached_data)}
            
            path = '/markets'
            method = "GET"
            
            params_map = {'limit': limit}
            if status == 'closed':
                params_map['closed'] = True
            else:
                params_map['closed'] = False
            
            url = f"{GAMMA_API_URL}{path}"
            
            result = send_request(method, url, params_map, headers={})
            result = json.loads(result) if isinstance(result, str) else result
            
            if isinstance(result, list):
                markets = result[:limit]
                if sort_by == 'volume':
                    markets = sorted(markets, key=lambda x: float(x.get('volume', 0)), reverse=True)
                elif sort_by == 'liquidity':
                    markets = sorted(markets, key=lambda x: float(x.get('liquidity', 0)), reverse=True)
                
                r.set(cache_key, json.dumps(markets), ex=10)
                return {'data': markets}
            
            return {'data': []}
        except Exception as e:
            logger_polymarket.error(f"get_active_markets error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def get_market_statistics(self, market_id):
        """
        Comprehensive market data snapshot. Features: combines market info + orderbook, spread/mid-price, 5sec cache.
        Use: market overview, pre-trade validation, risk assessment.
        
        Args:
            market_id (str): Market ID (condition ID)
            
        Returns:
            dict: question, volume, liquidity, prices, bid/ask, spread, resolution date or error
        """
        try:
            cache_key = f'polymarket_{market_id}_statistics'
            cached_data = r.get(cache_key)
            if cached_data is not None:
                return {'data': json.loads(cached_data)}
            
            market_info = self.get_market_info(market_id)
            orderbook = self.get_order_book_full(market_id)
            
            if market_info.get('data') and orderbook.get('data'):
                info = market_info['data']
                book = orderbook['data']
                
                stats = {
                    'market_id': market_id,
                    'question': info.get('question', ''),
                    'description': info.get('description', ''),
                    'outcome_type': info.get('outcomType', ''),
                    'volume': info.get('volume', '0'),
                    'liquidity': info.get('liquidity', '0'),
                    'last_price': info.get('lastPrice', '0'),
                    'last_price_yes': info.get('lastPriceYes', '0') if info.get('outcomType') == 'categorical' else None,
                    'last_price_no': info.get('lastPriceNo', '0') if info.get('outcomType') == 'categorical' else None,
                    'bid': book.get('bids', [{}])[0].get('price') if book.get('bids') else None,
                    'ask': book.get('asks', [{}])[0].get('price') if book.get('asks') else None,
                    'spread': self._calculate_spread(book.get('bids', []), book.get('asks', [])),
                    'mid_price': book.get('mid_price'),
                    'volume_24h': info.get('volume24h', '0'),
                    'resolution': info.get('resolution', ''),
                    'end_date': info.get('endDate', ''),
                    'neg_risk': book.get('neg_risk', False)
                }
                
                r.set(cache_key, json.dumps(stats), ex=5)
                return {'data': stats}
            
            return {'error': 'Market data not found', 'data': {}}
        except Exception as e:
            logger_polymarket.error(f"get_market_statistics error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def get_market_liquidity(self, market_ids=None):
        """
        Liquidity metrics (volume, 24h volume, price). Features: multi-market support, exit ability assessment.
        Use: filter illiquid markets, position exit analysis, liquidity scoring.
        
        Args:
            market_ids (list): List of market IDs to get liquidity for
            
        Returns:
            dict: liquidity, volume, volume_24h, price for each market or error
        """
        try:
            liquidity_data = {}
            
            if market_ids:
                for market_id in market_ids:
                    market_info = self.get_market_info(market_id)
                    if market_info.get('data'):
                        liquidity_data[market_id] = {
                            'liquidity': market_info['data'].get('liquidity', '0'),
                            'volume': market_info['data'].get('volume', '0'),
                            'volume_24h': market_info['data'].get('volume24h', '0'),
                            'price': market_info['data'].get('lastPrice', '0')
                        }
            
            return {'data': liquidity_data} if liquidity_data else {'data': {}}
        except Exception as e:
            logger_polymarket.error(f"get_market_liquidity error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def get_price_history(self, market_id, limit=100, before=''):
        """
        Historical price data from Gamma API. Features: time-series data, configurable limit.
        Use: trend analysis, backtesting, price pattern detection.
        
        Args:
            market_id (str): Market ID
            limit (int): Records per query
            before (str): Pagination timestamp (unused in Gamma API)
            
        Returns:
            dict: Historical price data or error
        """
        try:
            path = '/markets'
            method = "GET"
            
            params_map = {'id': market_id, 'limit': 1}
            
            url = f"{GAMMA_API_URL}{path}"
            
            result = send_request(method, url, params_map, headers={})
            result = json.loads(result) if isinstance(result, str) else result
            
            if isinstance(result, list) and len(result) > 0:
                market = result[0]
                history_data = {
                    'market_id': market.get('id', ''),
                    'price': float(market.get('lastPrice', 0)) if market.get('lastPrice') else 0,
                    'volume': float(market.get('volume', 0)) if market.get('volume') else 0,
                    'volume_24h': float(market.get('volume24hr', 0)) if market.get('volume24hr') else 0,
                    'timestamp': int(time.time() * 1000)
                }
                return {'data': [history_data]}
            
            return {'data': []}
        except Exception as e:
            logger_polymarket.error(f"get_price_history error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def search_markets(self, query, limit=20, offset=0):
        """
        Search markets by keyword from Gamma API. Features: configurable limit, filtering by question.
        Use: find specific markets, search by question, market discovery.
        
        Args:
            query (str): Search query string
            limit (int): Results per query (default 20)
            offset (int): Pagination offset (default 0)
            
        Returns:
            dict: Matching markets with pagination or error
        """
        try:
            path = '/markets'
            method = "GET"
            
            params_map = {'limit': limit + offset}
            
            url = f"{GAMMA_API_URL}{path}"
            
            result = send_request(method, url, params_map, headers={})
            result = json.loads(result) if isinstance(result, str) else result
            
            if isinstance(result, list):
                query_lower = query.lower()
                filtered = [m for m in result if query_lower in m.get('question', '').lower()]
                return {'data': filtered[offset:offset + limit]}
            
            return {'data': []}
        except Exception as e:
            logger_polymarket.error(f"search_markets error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def get_market_events(self, market_id, limit=50):
        """
        Get market events from Gamma API. Features: activity data, event metadata.
        Use: monitor market activity, event analysis.
        
        Args:
            market_id (str): Market ID
            limit (int): Events to retrieve (default 50)
            
        Returns:
            dict: Market events or error
        """
        try:
            path = '/events'
            method = "GET"
            
            params_map = {'limit': limit}
            
            url = f"{GAMMA_API_URL}{path}"
            
            result = send_request(method, url, params_map, headers={})
            result = json.loads(result) if isinstance(result, str) else result
            
            if isinstance(result, list):
                return {'data': result[:limit]}
            
            return {'data': []}
        except Exception as e:
            logger_polymarket.error(f"get_market_events error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': []}

    def get_market_history(self, market_id):
        """
        Complete market lifecycle data. Features: creation/resolution dates, outcomes, price history, 30sec cache.
        Use: research resolved markets, outcome analysis, historical study.
        
        Args:
            market_id (str): Market ID (condition ID)
            
        Returns:
            dict: question, outcomes, status, dates, volumes, initial/final prices or error
        """
        try:
            cache_key = f'polymarket_{market_id}_history'
            cached_data = r.get(cache_key)
            if cached_data is not None:
                return {'data': json.loads(cached_data)}
            
            market_info = self.get_market_info(market_id)
            
            if market_info.get('data'):
                data = market_info['data']
                history = {
                    'market_id': market_id,
                    'question': data.get('question', ''),
                    'outcome_type': data.get('outcomType', ''),
                    'status': data.get('status', ''),
                    'created_at': data.get('createdAt', ''),
                    'end_date': data.get('endDate', ''),
                    'resolved_at': data.get('resolvedAt', ''),
                    'resolution': data.get('resolution', ''),
                    'outcomes': data.get('outcomes', []),
                    'initial_price': data.get('initialPrice', '0'),
                    'last_price': data.get('lastPrice', '0'),
                    'total_volume': data.get('volume', '0'),
                    'total_liquidity': data.get('liquidity', '0')
                }
                
                r.set(cache_key, json.dumps(history), ex=30)
                return {'data': history}
            
            return {'error': 'Market history not found', 'data': {}}
        except Exception as e:
            logger_polymarket.error(f"get_market_history error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    
    def get_market_summary(self, market_id):
        """
        Ultimate market snapshot (combines all data). Features: complete market overview, fetch_timestamp, 5sec cache.
        Use: dashboard display, bot decision-making, market comparison.
        
        Args:
            market_id (str): Market ID (condition ID)
            
        Returns:
            dict: All market data (statistics + timestamp) or error
        """
        try:
            cache_key = f'polymarket_{market_id}_summary'
            cached_data = r.get(cache_key)
            if cached_data is not None:
                return {'data': json.loads(cached_data)}
            
            stats = self.get_market_statistics(market_id)
            
            if stats.get('data'):
                summary = {
                    **stats['data'],
                    'fetch_timestamp': str(int(time.time() * 1000))
                }
                
                r.set(cache_key, json.dumps(summary), ex=5)
                return {'data': summary}
            
            return stats
        except Exception as e:
            logger_polymarket.error(f"get_market_summary error {e} line: {e.__traceback__.tb_lineno}")
            return {'error': str(e), 'data': {}}

    def _calculate_mid_price(self, bids, asks):
        """
        Helper: (best_bid + best_ask) / 2. Features: safe error handling.
        Use: internal orderbook calculations.
        
        Args:
            bids (list): Bid orders with 'price' field
            asks (list): Ask orders with 'price' field
            
        Returns:
            float: Mid price or None if data unavailable
        """
        try:
            if bids and asks:
                best_bid = float(bids[0].get('price', 0))
                best_ask = float(asks[0].get('price', 0))
                if best_bid > 0 and best_ask > 0:
                    return (best_bid + best_ask) / 2
        except (ValueError, IndexError, TypeError):
            pass
        return None

    def _calculate_spread(self, bids, asks):
        """
        Helper: best_ask - best_bid. Features: safe error handling, liquidity measure.
        Use: internal spread calculations.
        
        Args:
            bids (list): Bid orders with 'price' field
            asks (list): Ask orders with 'price' field
            
        Returns:
            float: Spread amount or None if data unavailable
        """
        try:
            if bids and asks:
                best_bid = float(bids[0].get('price', 0))
                best_ask = float(asks[0].get('price', 0))
                if best_bid > 0 and best_ask > 0:
                    return best_ask - best_bid
        except (ValueError, IndexError, TypeError):
            pass
        return None

    