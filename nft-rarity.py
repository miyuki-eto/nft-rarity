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


def load_data(contract, update):
    if not os.path.exists('data'):
        os.makedirs('data')

    if (not os.path.isfile('data/' + contract + '.json')) or update:
        update_collection_metadata(contract)

    with open('data/' + contract + '.json') as json_file:
        collection_data = json.load(json_file)
    return collection_data


def calculate_rarity(data):
    rarity_data = []
    trait_names = []
    for asset in data:
        asset_dict = {'token_id': '' if 'token_id' not in asset else asset['token_id'],
                      'link': '' if 'permalink' not in asset else asset['permalink']}
        if len(asset['traits']) > 0:
            asset_dict['trait_count'] = len(asset['traits'])
            for trait in asset['traits']:
                asset_dict[trait['trait_type']] = trait['value']
                trait_names.append(trait['trait_type'])

        if asset['sell_orders'] is not None:
            asset_dict['price'] = float(asset['sell_orders'][0]['current_price']) / 10 ** float(asset['sell_orders'][0]['payment_token_contract']['decimals'])
            asset_dict['currency'] = asset['sell_orders'][0]['payment_token_contract']['symbol']

        rarity_data.append(asset_dict)
        trait_names = list(set(trait_names))

    fn_df = pd.DataFrame(rarity_data)
    fn_df = fn_df.fillna('-')

    fn_df['stat_rarity'] = 1
    fn_df['score_rarity'] = 0

    trait_names.append('trait_count')
    for trait_name in trait_names:
        trait_freq_score = dict(fn_df[trait_name].value_counts())
        fn_df[trait_name + '_score'] = 1 / (fn_df[trait_name].map(trait_freq_score) / len(fn_df))
        fn_df['score_rarity'] = fn_df['score_rarity'] + fn_df[trait_name + '_score']

    fn_df['score_rarity'] = fn_df['score_rarity']
    fn_df["score_rank"] = fn_df["score_rarity"].rank(ascending=False, method='min')
    fn_df.sort_values(['score_rarity'], inplace=True, ascending=[False])
    fn_df.reset_index(drop=True, inplace=True)
    fn_df['score_rank'] = fn_df['score_rank'].astype(int)
    return fn_df


collection = "0xB1bb22c3101E7653d0d969F42F831BD9aCCc38a5" #KitPics
# collection = "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d" #Bored Ape Yacht Club

# update_data = False
# raw_data = load_data(collection, update_data)
# df = calculate_rarity(raw_data)
#
# for_sale = df[df['price'] != '-'][['token_id', 'price', 'currency', 'link', 'score_rarity', 'score_rank']]
# print(for_sale)

# ORDERS
# import requests
#
# url = "https://api.opensea.io/wyvern/v1/orders?asset_contract_address=0xB1bb22c3101E7653d0d969F42F831BD9aCCc38a5&bundled=false&include_bundled=false&include_invalid=false&token_ids=4070&token_ids=5105&limit=50&offset=0&order_by=created_date&order_direction=desc"
#
# headers = {"Accept": "application/json"}
#
# response = requests.request("GET", url, headers=headers).json()
#
# pprint(response)
# print(len(response))

import requests

url = "https://api.opensea.io/api/v1/events?asset_contract_address=0xB1bb22c3101E7653d0d969F42F831BD9aCCc38a5&only_opensea=false&offset=0&limit=50"

headers = {"Accept": "application/json"}

response = requests.request("GET", url, headers=headers)

print(response.text)
