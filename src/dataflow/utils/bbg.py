

DATAFLOW_ID_TO_BBG = {
    'ICE.B':  'CO',  # Crude Oil, Brent
    'ICE.G':  'QS',  # Gas oil, ICE, Low Su, europe
    'CME.CL': 'CL',  # Crude Oil, WTI
    'CME.GC': 'GC',  # Gold
    'CME.HG': 'HG',  # Copper
    'CME.PL': 'PL',  # Platinum
    'CME.SI': 'SI',  # Silver
    'CME.PA': 'PA',  # Palladium
    'CME.RB': 'XB',  # RBOB
    'CME.HH': 'NG',  # Natural Gas
    'LME.AH': 'LA',  # Aluminum
    'LME.CA': 'LP',  # Copper
    'LME.NI': 'LN',  # Nickel
    'LME.PB': 'LL',  # Lead
    'LME.ZS': 'LX',  # Zinc
}


BBG_SYMBOL_SPEC = {
    'CL': {'asset_type': 'fut', 'bbg_root_id': 'CL', 'year_digit': 1, 'suffix': 'Comdty'},
    'CO': {'asset_type': 'fut', 'bbg_root_id': 'CO', 'year_digit': 1, 'suffix': 'Comdty'},
    'GC': {'asset_type': 'fut', 'bbg_root_id': 'GC', 'year_digit': 1, 'suffix': 'Comdty'},
    'HG': {'asset_type': 'fut', 'bbg_root_id': 'HG', 'year_digit': 2, 'suffix': 'Comdty'},
    'LA': {'asset_type': 'fut', 'bbg_root_id': 'LA', 'year_digit': 1, 'suffix': 'Comdty'},
    'LL': {'asset_type': 'fut', 'bbg_root_id': 'LL', 'year_digit': 1, 'suffix': 'Comdty'},
    'LN': {'asset_type': 'fut', 'bbg_root_id': 'LN', 'year_digit': 1, 'suffix': 'Comdty'},
    'LP': {'asset_type': 'fut', 'bbg_root_id': 'LP', 'year_digit': 2, 'suffix': 'Comdty'},
    'LX': {'asset_type': 'fut', 'bbg_root_id': 'LX', 'year_digit': 2, 'suffix': 'Comdty'},
    'NG': {'asset_type': 'fut', 'bbg_root_id': 'NP', 'year_digit': 1, 'suffix': 'Comdty'},
    'PA': {'asset_type': 'fut', 'bbg_root_id': 'PA', 'year_digit': 1, 'suffix': 'Comdty'},
    'PL': {'asset_type': 'fut', 'bbg_root_id': 'PL', 'year_digit': 1, 'suffix': 'Comdty'},
    'QS': {'asset_type': 'fut', 'bbg_root_id': 'QS', 'year_digit': 1, 'suffix': 'Comdty'},
    'SI': {'asset_type': 'fut', 'bbg_root_id': 'SI', 'year_digit': 1, 'suffix': 'Comdty'},
    'XB': {'asset_type': 'fut', 'bbg_root_id': 'XB', 'year_digit': 1, 'suffix': 'Comdty'},
    'LMHADS03': {'asset_type': 'forward', 'bbg_root_id': 'LMHADS03', 'year_digit': 0, 'suffix': 'Comdty'},
    'LMCADS03': {'asset_type': 'forward', 'bbg_root_id': 'LMCADS03', 'year_digit': 0, 'suffix': 'Comdty'},
    'LMMIDS03': {'asset_type': 'forward', 'bbg_root_id': 'LMMIDS03', 'year_digit': 0, 'suffix': 'Comdty'},
    'LMPBDS03': {'asset_type': 'forward', 'bbg_root_id': 'LMPBDS03', 'year_digit': 0, 'suffix': 'Comdty'},
    'LMZSDS03': {'asset_type': 'forward', 'bbg_root_id': 'LMZSDS03', 'year_digit': 0, 'suffix': 'Comdty'},
    'XAGUSD': {'asset_type': 'fx', 'bbg_root_id': 'XAGUSD', 'year_digit': 0, 'suffix': 'Currency'},
    'XAUUSD': {'asset_type': 'fx', 'bbg_root_id': 'XAUUSD', 'year_digit': 0, 'suffix': 'Currency'},
    'XPTUSD': {'asset_type': 'fx', 'bbg_root_id': 'XPTUSD', 'year_digit': 0, 'suffix': 'Currency'}
}