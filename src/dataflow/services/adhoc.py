import databento as db
from dataflow.config.settings import settings

db.enable_logging("INFO")

live_client = db.Live(settings.databento_api_key)
client = db.Historical(settings.databento_api_key)

def get_child_instruments():
    # Request definition data for parent symbols
    root_ids = ["GC.FUT"]
    data = client.timeseries.get_range(
        dataset="GLBX.MDP3",
        symbols=root_ids,
        stype_in="parent",
        schema="definition",
        start="2025-10-15",
    )
    data_df = data.to_df()
    print(data_df)

def get_all_symbols():
    data = client.timeseries.get_range(
        dataset="GLBX.MDP3",
        schema="definition",
        symbols="ALL_SYMBOLS",
        start="2025-10-17"
    )
    df = data.to_df()
    print(df)


def resolve_symbol():
    symbols = ["CL.c.0", "CL.c.1", "CL.c.2", "CL.c.3"]  # GC.c.0, GC.n.0, GC.v.0
    results = client.symbology.resolve(
        dataset="GLBX.MDP3",
        symbols=symbols,
        stype_in="continuous",
        stype_out="instrument_id",
        start_date="2025-10-20",
        end_date="2025-10-21",
    )
    for symbol in symbols:
        symbol_result = results["result"].get(symbol)
        if symbol_result:
            for meta in symbol_result:
                start = meta["d0"]
                end = meta["d1"]
                instrument_id = meta["s"]
                resp = client.symbology.resolve(
                    dataset="GLBX.MDP3",
                    symbols=[instrument_id],
                    stype_in="instrument_id",
                    stype_out="raw_symbol",
                    start_date=start,
                    end_date=end,
                )
                if resp["message"] == "OK":
                    resp_result = resp["result"][instrument_id]
                    for meta in resp_result:
                        raw_symbol = meta["s"]
                        print(f"{symbol} -> {raw_symbol}")



# get_child_instruments()

resolve_symbol()

# get_all_symbols()