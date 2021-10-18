import asyncio
import aiohttp
import aiolimiter
import pandas as pd
import datetime
from functions import make_api_call


async def async_event_calls(contract):
    session = aiohttp.ClientSession()
    limiter = aiolimiter.AsyncLimiter(max_rate=1, time_period=1)
    fn_results = []
    ids = []
    count = 0
    start_date = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%dT%H:%M:%S.%f")
    async with limiter:
        while True:
            fn_url = "https://api.opensea.io/api/v1/events?asset_contract_address=" + contract + "&occurred_before=" + start_date + "&only_opensea=false&offset=0&limit=50"
            print(fn_url)
            result = await make_api_call(session, limiter, fn_url)
            result = result['asset_events']

            if result[-1]['created_date'] != start_date:
                result = result[1:] if count > 0 else result
                for x in result:
                    if x['id'] not in ids:
                        fn_results.append(x)
                        ids.append(x['id'])
            else:
                break
            start_date = result[-1]['created_date']
            count += 1
    await session.close()
    return fn_results


def reduce_event_data(raw_data):
    reduced_data_all = []
    idx = 0
    for data in raw_data:
        if data['asset'] is None:
            for asset in data['asset_bundle']['assets']:
                reduced_data = {
                    'id': asset['id'],
                    'token_id': asset['token_id'],
                    'collection_name': asset['asset_contract']['name'],
                    'contract_address': asset['asset_contract']['address'],
                    'event_id': data['id'],
                    'created_date': data['created_date'],
                    'duration': data['duration'],
                    'event_type': data['event_type'],
                    'starting_price': data['starting_price'],
                    'ending_price': data['ending_price'],
                    'total_price': data['total_price'],
                    'payment_token': data['payment_token']['symbol'] if data['payment_token'] is not None else '',
                    'payment_token_decimals': data['payment_token']['decimals'] if data['payment_token'] is not None else '',
                    'seller_address': data['seller']['address'] if data['seller'] is not None else '',
                    'to_account': data['to_account']['address'] if data['to_account'] is not None else '',
                    'from_account': data['from_account']['address'] if data['from_account'] is not None else '',
                }
                reduced_data_all.append(reduced_data)
        else:
            # pprint(data)
            # print(idx)
            reduced_data = {
                'id': data['asset']['id'],
                'token_id': data['asset']['token_id'],
                'collection_name': data['asset']['asset_contract']['name'],
                'contract_address': data['asset']['asset_contract']['address'],
                'event_id': data['id'],
                'created_date': data['created_date'],
                'duration': data['duration'],
                'event_type': data['event_type'],
                'starting_price': data['starting_price'],
                'ending_price': data['ending_price'],
                'total_price': data['total_price'],
                'quantity': data['quantity'],
                'payment_token': data['payment_token']['symbol'] if data['payment_token'] is not None else '',
                'payment_token_decimals': data['payment_token']['decimals'] if data['payment_token'] is not None else '',
                'seller_account': data['seller']['address'] if data['seller'] is not None else '',
                'to_account': data['to_account']['address'] if data['to_account'] is not None else '',
                'from_account': data['from_account']['address'] if data['from_account'] is not None else '',
            }
            reduced_data_all.append(reduced_data)
        idx += 1
    return reduced_data_all


def fetch_collection_events(contract):
    results = asyncio.get_event_loop().run_until_complete(async_event_calls(contract))
    results = reduce_event_data(results)
    results = pd.DataFrame(results)
    numeric_cols = ['token_id', 'starting_price', 'ending_price', 'total_price', 'payment_token_decimals']
    for col in numeric_cols:
        results[col] = pd.to_numeric(results[col], errors='coerce')

    results['starting_price'] = results['starting_price'] / 10 ** results['payment_token_decimals']
    results['ending_price'] = results['ending_price'] / 10 ** results['payment_token_decimals']
    results['total_price'] = results['total_price'] / 10 ** results['payment_token_decimals']
    results.sort_values(['created_date'], inplace=True, ascending=[True])
    results.reset_index(drop=True, inplace=True)
    return results
