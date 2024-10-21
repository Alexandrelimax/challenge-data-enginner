import aiohttp
import pandas as pd

class APIFetch:
    def __init__(self, base_url):
        self.base_url = base_url

    async def fetch_data(self, endpoint):
        
        url = f"{self.base_url}/{endpoint}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()

