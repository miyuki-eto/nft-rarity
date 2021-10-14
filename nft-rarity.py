import asyncio
import aiohttp
import aiolimiter
import json
import math
import os
import pandas as pd
from pprint import pprint

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 2500)
pd.set_option("max_colwidth", 100)


async def make_api_call(aiohttp_session, fn_limiter, uri):
    async with fn_limiter:
        async with aiohttp_session.get(uri) as resp:
            response = await resp.text()

        return json.loads(response)['assets']


async def async_api_calls(contract):
    session = aiohttp.ClientSession()
    rate = 1
    period = 1
    limiter = aiolimiter.AsyncLimiter(rate, period)
    fn_urls = await get_url_list(session, limiter, contract)
    fn_results = []
    async with limiter:
        for url in fn_urls:
            print(url)
            result = await make_api_call(session, limiter, url)
            for x in result:
                fn_results.append(x)
    return fn_results


async def get_url_list(aiohttp_session, fn_limiter, contract):
    init_url = "https://api.opensea.io/api/v1/assets?&asset_contract_address=" + contract + "&order_direction=desc&offset=0&limit=1"
    init_call = await make_api_call(aiohttp_session, fn_limiter, init_url)
    array_length = math.ceil((int(init_call[0]['token_id']) + 1) / 50) - 1
    url_list = ["https://api.opensea.io/api/v1/assets?&asset_contract_address=" + contract + "&order_direction=desc&offset=" + str(50 * x) + "&limit=" + str(50) for x in range(array_length)]
    return url_list


def update_collection_metadata(contract):
    results = asyncio.get_event_loop().run_until_complete(async_api_calls(contract))

    with open('data/' + contract + '.json', 'w') as outfile:
        json.dump(results, outfile)


def load_data(contract):
    if not os.path.exists('data'):
        os.makedirs('data')

    if not os.path.isfile('data/' + contract + '.json'):
        update_collection_metadata(contract)

    with open('data/' + contract + '.json') as json_file:
        collection_data = json.load(json_file)
    return collection_data


def calculate_rarity(data):
    rarity_data = []
    trait_names = []
    for asset in data:
        asset_dict = {}
        asset_dict['token_id'] = '' if 'token_id' not in asset else asset['token_id']
        asset_dict['link'] = '' if 'permalink' not in asset else asset['permalink']
        if len(asset['traits']) > 0:
            asset_dict['trait_count'] = len(asset['traits'])
            for trait in asset['traits']:
                asset_dict[trait['trait_type']] = trait['value']
                trait_names.append(trait['trait_type'])

        rarity_data.append(asset_dict)
        trait_names = list(set(trait_names))

    df = pd.DataFrame(rarity_data)
    df = df.fillna('-')

    df['stat_rarity'] = 1

    trait_names.append('trait_count')
    for trait_name in trait_names:
        trait_freq = dict(df[trait_name].value_counts(normalize=True) * 100)
        df[trait_name] = df[trait_name].map(trait_freq)
        df['stat_rarity'] = df['stat_rarity'] * df[trait_name]

    df['stat_rarity'] = df['stat_rarity'] * 100
    df["stat_rank"] = df["stat_rarity"].rank(ascending=True, method='min')
    df.sort_values(['stat_rarity'], inplace=True, ascending=[True])
    df.reset_index(drop=True, inplace=True)
    return df


collection = "0xB1bb22c3101E7653d0d969F42F831BD9aCCc38a5"
raw_data = load_data(collection)
df = calculate_rarity(raw_data)

print(df.head(20))
