import requests
import logging
from collections import defaultdict

from dataflow.config.settings import settings
from dataflow.utils.common import parse_web_response
from dataflow.symbology.base import BaseSymbolResolver

logger = logging.getLogger(__name__)


class OnyxSymbolResolve(BaseSymbolResolver):
    def __init__(self,
                 symbol_type_in: str = "product_id",
                 symbol_type_out: str = "raw_symbol",
                 ):
        self.symbol_type_in = symbol_type_in
        self.symbol_type_out = symbol_type_out
        self.headers = {
            "Authorization": f"Bearer {settings.onyx_api_key}",
        }

    def resolve(self, input_symbols: list[str]) -> dict[str, str]:
        final_map = {}

        groups = defaultdict(list)
        for sym in input_symbols:
            parts = sym.split('.')
            if len(parts) == 3:
                product = parts[1]
                groups[product].append(sym)
            else:
                raise ValueError(f"Onyx series id must consist of 3 parts: {sym}")

        for product in groups:
            groups[product].sort(key=lambda x: int(x.split('.')[2]))

        for product_name, input_symbols in groups.items():
            params = {
                "product_symbol": product_name,
            }
            url = f"{settings.onyx_url}/contracts"
            resp = requests.get(url, headers=self.headers, params=params)
            data, error = parse_web_response(resp)
            if error:
                logger.error(f"Failed to all futures data for {product_name}: {error}")
            else:
                for d in data:
                    pass


onyx_symbol_resolver = OnyxSymbolResolve()


if __name__ == "__main__":
    mapping = onyx_symbol_resolver.resolve(["ONYX.NAPEW.1", "ONYX.NAPEW.2", "ONYX.NAPEW.3"])
    print(mapping)