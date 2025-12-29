

import json
import os
import sys
import time
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from polymarket_private import PolymarketPrivate
from py_clob_client.clob_types import BalanceAllowanceParams, AssetType


API_KEY = "db1bfcd6-2f51-3453-99b3-6e1f6c0a6e69"
SECRET_KEY = "_RjZeDEcY7z7xbAhGXR54SeqhD0tbth6YaS4sKryF-8="
PASSPHRASE = "b346d94c2fc83e0a46c0a87f15c601b48ee198342478531e28e3295942a69f70"

WALLET_ADDRESS = "0xBa66797f1a40107f652c9e91e4f44b7f7d924458" #metamask
PRIVATE_KEY = "341a20b891ce7a11cebeb981562eb536441b50554d15bfab537f9ebde857b6d0"  #metamask
PROXY_ADDRESS = "0xd0dE444431FcfFF4C538bE8949Cc01025cB260e1"  #vi deposit

MARKET_ID = '601698' 
TAG_ID = 1

def main():
    client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE, PRIVATE_KEY, PROXY_ADDRESS)
    client_proxy = PolymarketPrivate(API_KEY, SECRET_KEY, PROXY_ADDRESS, PASSPHRASE, PRIVATE_KEY)
    
 

    # Test get_account_balance with signature_type=2 (GNOSIS_SAFE)
    try:
        print(f"\nChecking account balance for Proxy {PROXY_ADDRESS} with signature_type=2 (GNOSIS_SAFE)...")
        result = client_proxy.get_account_balance(signature_type=2, funder=PROXY_ADDRESS)
        print(f"get_account_balance (GNOSIS_SAFE):")
        print(json.dumps(result, indent=2))
        
        if 'data' in result:
            data = result['data']
            total_usdc = float(data.get('total', 0)) / 1e6
            available_usdc = float(data.get('available', 0)) / 1e6
          
    except Exception as e:
        print(f"Error checking account balance (GNOSIS_SAFE): {e}")
        import traceback
        traceback.print_exc()

    # client_proxy = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE, PRIVATE_KEY,PROXY_ADDRESS)
    # try:
    #     # print(f"\nAttempting to place order V2 using Proxy Wallet: {PROXY_ADDRESS}")
    #     # print(f"Signer (EOA): {PRIVATE_KEY[:6]}... (derived from private key)")
        
    #     result = client_proxy.place_order_v2(
    #         market_id=MARKET_ID, 
    #         side='BUY', 
    #         size='5',  # Reduced size to be within balance
    #         price='0.1', 
    #         order_type="LIMIT",
    #         token_index=0,
    #         signature_type=2 # 2 = GNOSIS_SAFE / Browser Wallet Proxy
    #     )
    #     print(f"place_order_v2 result: {result}")
        
    # except Exception as e:
    #     print(f"Error placing proxy order v2: {e}")

    # print("\n" + "="*80)
    # print("TESTING PLACE ORDER WITH EOA (MAIN WALLET)")
    # print("="*80)


    # client_proxy = PolymarketPrivate(API_KEY, SECRET_KEY, PROXY_ADDRESS, PASSPHRASE, PRIVATE_KEY)

    # try:
        
    #     result = client_proxy.place_order(
    #         market_id=MARKET_ID, 
    #         side='BUY', 
    #         size='5',  # Increased size to meet min $1
    #         price='0.1', 
    #         token_index=0,
    #         signature_type=2 # 2 = GNOSIS_SAFE / Browser Wallet Proxy
    #     )
    #     print(f"place_order result place_order: {result}")
        
    # except Exception as e:
    #     print(f"Error placing proxy order: {e}")


    try:    
        result = client_proxy.place_order_(
            token_id="87142374061000861551796777015507582795011030012469790923844501037328154887367", 
            side='buy', 
            size='5',  # Increased size to meet min $1
            price='0.4', 
            signature_type=2 # 2 = GNOSIS_SAFE / Browser Wallet Proxy
        )
        print(f"place_order result place_order: {result}")
        
    except Exception as e:
        print(f"Error placing proxy order: {e}")


    # # Test get_open_orders (requires auth)
    # try:
    #     result = client.get_open_orders()
    #     print("get_open_orders:", result)
    #     open_orders = result.get('data', [])
    # except Exception as e:
    #     print("Error getting get_open_orders:", e)
    #     open_orders = []

    # if open_orders and len(open_orders) > 0:
    #     actual_order_id = open_orders[0].get('id')
    #     try:
    #         result = client.get_order_details(actual_order_id)
    #         print("get_order_details:", result)
    #     except Exception as e:
    #         print("Error getting get_order_details:", e)
        
    # else:
    #     print("No open orders found to test get_order_details and cancel_order")


    # #Test Cancel Order
    # try:
    #     result = client.cancel_order("0x0261ab4ec1a215583d85da5a60d8ac1ae13dec89035f553da9b1ff70775098fd")
    #     print("cancel_order:", result)
    # except Exception as e:
    #     print("Error getting cancel_order:", e) 

    # # Test cancel_orders (cancel all open orders)
    # try:
    #     result = client.cancel_orders()
    #     print("cancel_orders:", result)
    # except Exception as e:
    #     print("Error getting cancel_orders:", e)

    # print("\n" + "="*80)
    # print("CHECKING WHICH WALLET HAS CLOB BALANCE")
    # print("="*80)
    
    # # Test get_account_balance for MAIN WALLET (requires auth)
    
    # Test place_batch_orders (requires auth - requires pre-signed orders)
    # Reference: https://docs.polymarket.com/developers/CLOB/orders/create-order-batch
    # try:
    #     orders_list = [
    #         {
    #             'order': {
    #                 'salt': '1',
    #                 'maker': WALLET_ADDRESS,
    #                 'signer': WALLET_ADDRESS,
    #                 'taker': '0x0000000000000000000000000000000000000000',
    #                 'tokenId': 'token_id',
    #                 'makerAmount': '1000000',
    #                 'takerAmount': '500000',
    #                 'expiration': '0',
    #                 'nonce': '0',
    #                 'feeRateBps': '0',
    #                 'side': '0',
    #                 'signatureType': '1',
    #                 'signature': 'sig_hex'
    #             },
    #             'orderType': 'GTC',
    #             'owner': API_KEY
    #         }
    #     ]
    #     result = client.place_batch_orders(orders_list)
    #     print("place_batch_orders:", result)
    # except Exception as e:
    #     print("Error getting place_batch_orders:", e)

   
    # Test get_user_trades
    # try:
    #     result = client.get_user_trades(limit=100)
    #     print("get_user_trades:", result)
    # except Exception as e:
    #     print("Error getting get_user_trades:", e)


    # Test get_topics
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_topics(limit=100)
    #     print("get_topics:", result)
    # except Exception as e:
    #     print("Error getting get_topics:", e)

    # Test get_rates_by_topic
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_rates_by_topic()
    #     print("get_rates_by_topic:", result)
    # except Exception as e:
    #     print("Error getting get_rates_by_topic:", e)

    # Test get_topic_statistics
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_topic_statistics()
    #     print("get_topic_statistics:", result)
    # except Exception as e:
    #     print("Error getting get_topic_statistics:", e)

    # Test get_market_rates ##
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_market_rates(MARKET_IDS)
    #     print("get_market_rates:", result)
    # except Exception as e:
    #     print("Error getting get_market_rates:", e)

    # # Test get_market_info
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_market_info(MARKET_ID)
    #     print("get_market_info:", result)
    # except Exception as e:
    #     print("Error getting get_market_info:", e)

    # # Test get_orderbook ## Fixinggggg
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_orderbook(MARKET_ID)
    #     print("get_orderbook:", result)
    # except Exception as e:
    #     print("Error getting get_orderbook:", e)

    # Test get_ticker ##undone
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_ticker(MARKET_ID)
    #     print("get_ticker:", result)
    # except Exception as e:
    #     print("Error getting get_ticker:", e)

    # # Test get_order_book_full ##undone
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_order_book_full(MARKET_ID)
    #     print("get_order_book_full:", result)
    # except Exception as e:
    #     print("Error getting get_order_book_full:", e)

    # # Test get_order_book_depth ##undone
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_order_book_depth(MARKET_ID, depth=10)
    #     print("get_order_book_depth:", result)
    # except Exception as e:
    #     print("Error getting get_order_book_depth:", e)

    # # Test get_market_prices ##undone
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_market_prices(MARKET_IDS)
    #     print("get_market_prices:", result)
    # except Exception as e:
    #     print("Error getting get_market_prices:", e)

    # # Test get_market_spreads ##undone
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_market_spreads(MARKET_IDS)
    #     print("get_market_spreads:", result)
    # except Exception as e:
    #     print("Error getting get_market_spreads:", e)

    # # Test get_active_markets
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_active_markets(status='active', sort_by='volume', limit=100)
    #     print("get_active_markets:", result)
    # except Exception as e:
    #     print("Error getting get_active_markets:", e)

    # Test get_market_statistics
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_market_statistics(MARKET_ID)
    #     print("get_market_statistics:", result)
    # except Exception as e:
    #     print("Error getting get_market_statistics:", e)

    # Test get_market_liquidity
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_market_liquidity(MARKET_IDS)
    #     print("get_market_liquidity:", result)
    # except Exception as e:
    #     print("Error getting get_market_liquidity:", e)

    # Test get_price_history
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_price_history(MARKET_ID, limit=100)
    #     print("get_price_history:", result)
    # except Exception as e:
    #     print("Error getting get_price_history:", e)

    # # Test search_markets
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.search_markets('bitcoin', limit=20)
    #     print("search_markets:", result)
    # except Exception as e:
    #     print("Error getting search_markets:", e)

    # Test get_market_events
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_market_events(MARKET_ID, limit=50)
    #     print("get_market_events:", result)
    # except Exception as e:
    #     print("Error getting get_market_events:", e)

    # # Test get_market_history
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_market_history(MARKET_ID)
    #     print("get_market_history:", result)
    # except Exception as e:
    #     print("Error getting get_market_history:", e)

    # # Test get_multiple_orderbooks ##undone
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_multiple_orderbooks(MARKET_IDS)
    #     print("get_multiple_orderbooks:", result)
    # except Exception as e:
    #     print("Error getting get_multiple_orderbooks:", e)

    # Test get_market_summary
    # client = PolymarketPrivate(API_KEY, SECRET_KEY, WALLET_ADDRESS, PASSPHRASE)
    # try:
    #     result = client.get_market_summary(MARKET_ID)
    #     print("get_market_summary:", result)
    # except Exception as e:
    #     print("Error getting get_market_summary:", e)

    # pass


    # print("\n" + "="*80)
    # print("TESTING get_balance_allowance")
    # print("="*80)
    
    # try:
    #     from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        
    #     print("\n1. Testing get_balance_allowance with default params (COLLATERAL):")
    #     result = client_main.get_balance_allowance()
    #     print(f"   Result: {result}")
        
    #     print("\n2. Testing get_balance_allowance with explicit params (COLLATERAL):")
    #     params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    #     result = client_main.get_balance_allowance(params)
    #     print(f"   Result: {result}")
        
    #     print("\n3. Testing get_balance_allowance with CONDITIONAL asset type:")
    #     params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL)
    #     result = client_main.get_balance_allowance(params)
    #     print(f"   Result: {result}")
        
    # except ImportError:
    #     print("   Skipping detailed params test because py_clob_client is not available in test script context")
    #     result = client_main.get_balance_allowance()
    #     print(f"   Result (default): {result}")
    # except Exception as e:
    #     print(f"   Error: {e}")


if __name__ == "__main__":
    main()
