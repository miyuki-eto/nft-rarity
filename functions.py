import json


async def make_api_call(aiohttp_session, fn_limiter, uri):
    async with fn_limiter:
        async with aiohttp_session.get(uri) as resp:
            response = await resp.text()
        return json.loads(response)
