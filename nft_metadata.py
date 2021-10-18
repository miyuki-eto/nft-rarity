import asyncio
import aiohttp
import aiolimiter
import math
import pandas as pd
from itertools import compress
from functions import make_api_call


async def get_url_list(aiohttp_session, fn_limiter, contract):
    init_url = "https://api.opensea.io/api/v1/assets?&asset_contract_address=" + contract + "&order_direction=desc&offset=0&limit=1"
    init_call = await make_api_call(aiohttp_session, fn_limiter, init_url)
    init_call = init_call['assets']
    array_length = math.ceil((int(init_call[0]['token_id']) + 1) / 50)
    url_list = ["https://api.opensea.io/api/v1/assets?&asset_contract_address=" + contract + "&order_direction=desc&offset=" + str(50 * x) + "&limit=" + str(50) for x in range(array_length)]
    return url_list


async def async_metadata_calls(contract):
    session = aiohttp.ClientSession()
    limiter = aiolimiter.AsyncLimiter(max_rate=1, time_period=1)
    fn_urls = await get_url_list(session, limiter, contract)
    fn_results = []
    async with limiter:
        for url in fn_urls:
            print(url)
            result = await make_api_call(session, limiter, url)
            for x in result['assets']:
                fn_results.append(x)
    await session.close()
    return fn_results


def reduce_metadata(raw_data):
    reduced_data_all = []
    traits = []
    for data in raw_data:
        for trait in data['traits']:
            traits.append(trait['trait_type'])
    trait_dict = dict(zip(traits, list(range(len(sorted(list(set(traits))))))))
    for data in raw_data:
        reduced_data = {
            'id': data['id'],
            'token_id': data['token_id'],
            'token_name': data['name'],
            'permalink': data['permalink'],
            'trait_qty': len(data['traits']),
            'collection_name': data['asset_contract']['name'],
            'contract_address': data['asset_contract']['address'],
            'date_created': data['asset_contract']['created_date'],
        }
        for trait in data['traits']:
            reduced_data['trait_' + str(trait_dict[trait['trait_type']]) + '_type'] = trait['trait_type']
            reduced_data['trait_' + str(trait_dict[trait['trait_type']]) + '_value'] = trait['value']
        reduced_data_all.append(reduced_data)
    return reduced_data_all


def calculate_rarity(data):
    trait_cols = data.columns.str.contains(r'trait_\d+_value')
    trait_col_names = list(compress(data.columns, trait_cols))
    trait_col_names.append('trait_qty')
    data['rarity_score'] = 0
    for trait_name in trait_col_names:
        trait_freq_score = dict(data[trait_name].value_counts())
        score = 1 / (data[trait_name].map(trait_freq_score) / len(data))
        data['rarity_score'] = data['rarity_score'] + score
    data["rarity_rank"] = data["rarity_score"].rank(ascending=False, method='min')
    data['rarity_rank'] = data['rarity_rank'].astype(int, errors='ignore')
    return data


def fetch_collection_metadata(contract):
    results = asyncio.get_event_loop().run_until_complete(async_metadata_calls(contract))
    results = reduce_metadata(results)
    results = pd.DataFrame(results)
    results['token_id'] = results['token_id'].astype(int)
    results.sort_values(['token_id'], inplace=True, ascending=[True])
    results = calculate_rarity(results)
    return results
