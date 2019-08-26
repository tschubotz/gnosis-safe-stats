from web3 import Web3
import json
import requests
import os
from dateutil import parser as date_parser
import datetime
from collections import OrderedDict 

STATS_FILENAME = ''

TOKENS_TO_IGNORE = [
    '0x055B283aDB49ea398162FDD50E0267d43b51B5E2',
    '0x2995a8c8d8C408beB565C74a0A9730088A1891D1',
    '0x30B8d24688991F0f0E6270913182c97A533A85fB',
    '0xF506828B166de88cA2EDb2A98D960aBba0D2402A',
    '0xF1E5F03086e1c0Ce55E54cd8146BC9c28435346F',
    '0xEd6cA9522FdA3b9Cd93025780A2939bd04a7ECD6',
    '0xe7049114562C759d5E9D1d25783773Ccd61C0a65',
    '0xd5524179cB7AE012f5B642C1D6D700Bbaa76B96b',
    '0xd33a62F420b29882668fc68FA6b08F2b1fEE1004', 
    '0xCFC64d8eAEB4E6a68860e415f55DFe9057dA7d2D',
    '0xADE1b7955252c379dc4399D5CD609A9cac1686e5',
    '0xA2881A90Bf33F03E7a3f803765Cd2ED5c8928dFb',
    '0x9dfe4643C04078a46803edCC30a3291b76d4c20c',
    '0x9CEc686ba6f07D6135B2091140c795166Ef5b761',
    '0x9c54A6e9f7E8cBc3690B6Ba1cEb75C44B58c2c83',
    '0x9BFEDc30A3930b709c0FCB01c5c59733b64aC827',
    '0x98976A6dFaAf97B16a4Bb06035cC84be12e79110',
    '0x919D0131fA5F77D99FBBBBaCe50bCb6E62332bf2',
    '0x8a67586c84c18F39106C3c01FDF0bF03889717f4',
    '0x85ace0444e03C652aC12838435B4FfCEdcc7714f',
    '0x85332b222787EacAb0fFf68cf3b884798823528C',
    '0x406F695614e09F46d62968aB7F7CE3E25377FBd1',
    '0x3E8d0c73dA23a3911C0F6AAb03397Fec3047f5bb',
    '0x37a74F5E05479EaC97Aba4A41A0daA021Bb59ecC',
    '0x4FbB350052Bca5417566f188eB2EBCE5b19BC964',
    '0x573D142775CB43b7a9081FC4f5C0731C0fEB8535',
    '0x5aCD07353106306a6530ac4D49233271Ec372963',
    '0x4c1C4957D22D8F373aeD54d0853b090666F6F9De',
    '0x552d72f86f04098a4eaeDA6D7b665AC12f846AD2',
    '0x6ff313FB38d53d7A458860b1bf7512f54a03e968',
    '0x06e0feB0D74106c7adA8497754074D222Ec6BCDf',  # BitBall, price >0 
    '0x05f02507c7134Dbae420AB8C0Ef56E999B59dA03',
    '0x11166C00AA9096E9B19A5C218DB86305Fe898b9C',  
    '0x0b22aB9d1D544463694Bd31EFb85166b5577fc23',
    '0x53Ad8c733a4338e2B8C235ECfDbed0Ef8f79C7Bd',
    '0x2F6CB07D35571Ca1Ba4fbEbC5a4EC68808Ff44e6',
    '0xD9A12Cde03a86E800496469858De8581D3A5353d',
    '0x216Ab78397Be269016BA6b95e746E5EcE536fA4a', 
    '0x466850472840C252c75B9077495587Ca97394e13',
    '0xad5BBB92E00A8DCE476555Fbb7FD54Bd287De2b3',  #ERC721
    '0x22C1f6050E56d2876009903609a2cC3fEf83B415',  #ERC721
    '0xdceaf1652a131F32a821468Dc03A92df0edd86Ea',  #ERC721
    '0xa1eB40c284C5B44419425c4202Fa8DabFF31006b',  #ERC721
    '0xFaC7BEA255a6990f749363002136aF6556b31e04',  #ENS, ERC721
    '0x477Ae5250A8B587Ad85EC1e7eC15C0518d0a4043',  #ERC721
    '0x2aEa4Add166EBf38b63d09a75dE1a7b94Aa24163',  #Kudos, ERC721
    '0x2F4Bdafb22bd92AA7b7552d270376dE8eDccbc1E',  #ERC721
    '0xD39fEa299BCc8a359D56FD9d148C4bcE89cb83b9',
    '0x1C3BB10dE15C31D5DBE48fbB7B87735d1B7d8c32',  
    '0x194C66AFbE810600A33d80Ae276Bcd2556063b37',

]

FORMER_PAYMENT_TOKENS = [
    '0x255Aa6DF07540Cb5d3d297f0D0D4D84cb52bc8e6'
]

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


class Stats(object):
    def __init__(self, start_date, end_date):
        self._data = OrderedDict()
        self._default_stats_values = OrderedDict()
        self._stats_keys = []

        for date in daterange(start_date, end_date + datetime.timedelta(days=1)):
            self._data[str(date)] = OrderedDict()

    def addDefaultStatsValue(self, stats_key, value):
        if not stats_key in self._stats_keys:
            self.addStatsKey(stats_key)
            
        self._default_stats_values[stats_key] = value

    def getDefaultStatsValue(self, stats_key):
        return self._default_stats_values.get(stats_key, 0)

    def addStatsKey(self, stats_key):
        self._stats_keys.append(stats_key)

    def add(self, date_key, stats_key, stats_value):
        if not stats_key in self._stats_keys:
            self.addStatsKey(stats_key)

        self._data[date_key][stats_key] = stats_value

    def getAllStatsKeys(self):
        return self._stats_keys

    def getStatsValue(self, date_key, stats_key):
        if not stats_key in self._data[date_key]:
            return self.getDefaultStatsValue(stats_key)
        return self._data[date_key][stats_key]

    def allData(self):
        for date_key in self._data.keys():
            out = [date_key]
            for stats_key in self.getAllStatsKeys():
                out.append(self.getStatsValue(date_key, stats_key))
            yield out



class TokenInfo(object):
    def __init__(self):
        self._tokens = OrderedDict()
        self._payment_tokens = OrderedDict()
        token_response = requests.get('https://safe-relay.gnosis.pm/api/v1/tokens/?limit=500')
        for token in token_response.json().get('results'):
            self._tokens[token['address']] = token
            if token['gas'] or token['address'] in FORMER_PAYMENT_TOKENS:
                self._payment_tokens[token['address']] = token
    
    def knows(self, address):
        knows = address in self._tokens.keys()
        if not knows and address not in TOKENS_TO_IGNORE: 
            print('Could not find info for token at https://etherscan.io/token/{}'.format(address))
        return knows

    def knowsPaymentToken(self, address):
        knows = address in self._payment_tokens.keys()
        if not knows and address not in TOKENS_TO_IGNORE:
            print('Could not find info for GAS token at https://etherscan.io/token/{}'.format(address))
        return knows

    def getSymbolForAddress(self, address):
        # Special handling for LRC since it exists in 2 versions. #hacky.
        if address == '0xBBbbCA6A901c926F240b89EacB641d8Aec7AEafD':
            return 'LRCv2'
        return self._tokens[address]['symbol']

    def allTokenSymbols(self):
        for _, token_info in self._tokens.items():
            yield token_info['symbol']

    def allPaymentTokenSymbols(self):
        for _, token_info in self._payment_tokens.items():
            yield token_info['symbol']

    def getDecimalsForAddress(self, address):
        return self._tokens[address]['decimals']

    

def main():
    if os.path.exists(STATS_FILENAME):
        print('{} exists. Please delete or choose different filename.'.format(STATS_FILENAME))
        exit(0)

    start_date = datetime.date(2018, 1, 1)
    end_date = datetime.date.today()
 
    stats = Stats(start_date, end_date)
    token_info = TokenInfo()

    # Get history stats
    response = requests.get('https://safe-relay.gnosis.pm/api/v1/stats/history/?fromDate={}'.format(start_date))
    

    # Safe deployments
    safe_deployments = response.json().get('safesCreated').get('deployed')
    for item in safe_deployments:
        stats.add(item['createdDate'], 'safes_created', item['number'])

    # Safe deployment times
    avg_safe_deployment_times = response.json().get('safesCreated').get('averageDeployTimeSeconds')
    stats.addDefaultStatsValue('avg_safe_creation_time', '')
    for item in avg_safe_deployment_times:
        stats.add(item['createdDate'], 'avg_safe_creation_time', float(item['averageDeployTime']))


    # PaymentTokens for deployment
    payment_token_num_deployment_txs = response.json().get('safesCreated').get('paymentTokens')
    
    stats.addStatsKey('payment_eth_creation')
    for symbol in token_info.allPaymentTokenSymbols():
        stats.addStatsKey('payment_{}_creation'.format(symbol))

    for item in payment_token_num_deployment_txs:
        gas_token_address = item['paymentToken']
        if gas_token_address is None:
            stats.add(item['date'],'payment_eth_creation', item['number'])
        elif not token_info.knowsPaymentToken(gas_token_address):
            continue
        else:
            stats_key = 'payment_{}_creation'.format(token_info.getSymbolForAddress(gas_token_address))
            stats.add(item['date'], stats_key, item['number'])


    # Funds stored ETH
    funds_stored_eth = response.json().get('safesCreated').get('fundsStored').get('ether')
    for item in funds_stored_eth:
        stats.add(item['date'], 'eth_stored', item['balance'] / 10**18)


    # Funds stored tokens
    funds_stored_tokens = response.json().get('safesCreated').get('fundsStored').get('tokens')
    for symbol in token_info.allTokenSymbols():
        stats.addStatsKey('{}_stored'.format(symbol))
    
    for item in funds_stored_tokens:
        token_address = item['tokenAddress']
        if not token_info.knows(token_address):
            continue
        stats_key = '{}_stored'.format(token_info.getSymbolForAddress(token_address))
        balance = item['balance'] / 10 ** token_info.getDecimalsForAddress(token_address)
        stats.add(item['date'], stats_key, balance)


    # Number of transactions
    transactions = response.json().get('relayedTxs').get('total')
    for item in transactions:
        stats.add(item['createdDate'], 'txs', item['number'])

    # Average transaction times
    avg_execution_times = response.json().get('relayedTxs').get('averageExecutionTimeSeconds')
    stats.addDefaultStatsValue('avg_tx_time', '')
    for item in avg_execution_times:
        stats.add(item['createdDate'], 'avg_tx_time', float(item['averageExecutionTime']))

    # Ether volume
    ether_volume = response.json().get('relayedTxs').get('volume').get('ether')
    for item in ether_volume:
        stats.add(item['date'], 'volume_eth', int(item['value']) / (10**18))

    # Token volumes
    token_volumes = response.json().get('relayedTxs').get('volume').get('tokens')
    for symbol in token_info.allTokenSymbols():
        stats.addStatsKey('volume_{}'.format(symbol))
    
    for item in token_volumes:
        token_address = item['tokenAddress']
        if not token_info.knows(token_address):
            continue
        stats_key = 'volume_{}'.format(token_info.getSymbolForAddress(token_address))
        balance = item['value'] / 10 ** token_info.getDecimalsForAddress(token_address)
        stats.add(item['date'], stats_key, balance)

    # Payment tokens
    payment_token_num_txs = response.json().get('relayedTxs').get('paymentTokens')
    
    stats.addStatsKey('payment_eth_txs')
    for symbol in token_info.allPaymentTokenSymbols():
        stats.addStatsKey('payment_{}_txs'.format(symbol))

    for item in payment_token_num_txs:
        gas_token_address = item['gasToken']
        if gas_token_address is None:
            stats.add(item['date'],'payment_eth_txs', item['number'])
        elif not token_info.knowsPaymentToken(gas_token_address):
            continue
        else:
            stats_key = 'payment_{}_txs'.format(token_info.getSymbolForAddress(gas_token_address))
            stats.add(item['date'], stats_key, item['number'])


    # Write output
    columns = ['date']
    columns.extend(stats.getAllStatsKeys())

    known_columns = ['date', 'safes_created', 'avg_safe_creation_time', 'payment_eth_creation', 'payment_OWL_creation', 'payment_DAI_creation', 'payment_WETH_creation', 'payment_KNC_creation', 'payment_MKR_creation', 'payment_RDN_creation', 'eth_stored', 'GNO_stored', 'OWL_stored', 'DAI_stored', 'MGN_stored', 'WETH_stored', '0xBTC_stored', 'ZRX_stored', 'ADX_stored', 'ADT_stored', 'AE_stored', 'AION_stored', 'AST_stored', 'ALIS_stored', 'AMB_stored', 'ANT_stored', 'APIS_stored', 'REP_stored', 'AOA_stored', 'BNT_stored', 'BAT_stored', 'BAX_stored', 'BCAP_stored', 'BZNT_stored', 'BHPC_stored', 'BSOV_stored', 'BCT_stored', 'BQX_stored', 'BIX_stored', 'BKN_stored', 'VEE_stored', 'BLZ_stored', 'BNB_stored', 'BOOTY_stored', 'BNTY_stored', 'BRD_stored', 'BRZC_stored', 'BCZERO_stored', 'BTM_stored', 'CAN_stored', 'CENNZ_stored', 'CHX_stored', 'CND_stored', 'cZRX_stored', 'cREP_stored', 'cBAT_stored', 'cDAI_stored', 'cETH_stored', 'cUSDC_stored', 'CTXC_stored', 'CS_stored', 'CREDO_stored', 'CRO_stored', 'CPT_stored', 'C20_stored', 'AUTO_stored', 'CVC_stored', 'CMT_stored', 'GEN_stored', 'DPP_stored', 'DATA_stored', 'XD_stored', 'DAY_stored', 'MANA_stored', 'DENT_stored', 'DCN_stored', 'DESH_stored', 'DGTX_stored', 'DGD_stored', 'DNT_stored', 'DMT_stored', 'DRGN_stored', 'EDG_stored', 'LEND_stored', 'EDO_stored', 'ELF_stored', 'EDR_stored', 'ENG_stored', 'ENJ_stored', 'ERC20_stored', 'XET_stored', 'ETHS_stored', 'NEC_stored', 'ETHPLO_stored', 'FOAM_stored', 'FUN_stored', 'FSN_stored', 'GUSD_stored', 'GNX_stored', 'GVT_stored', 'GTO_stored', 'GIM_stored', 'GNT_stored', 'GRID_stored', 'GSE_stored', 'HOT_stored', 'HPB_stored', 'HT_stored', 'HYPE_stored', 'ICN_stored', 'ICX_stored', 'IDEX_stored', 'RLC_stored', 'INS_stored', 'INB_stored', 'IOST_stored', 'IOTX_stored', 'JNT_stored', 'KIN_stored', 'KNC_stored', 'LINK (Chainlink)_stored', 'LKY_stored', 'LPT_stored', 'LOOM_stored', 'LRC_stored', 'LRC_stored', 'MFT_stored', 'MKR_stored', 'MAN_stored', 'MXM_stored', 'MCO_stored', 'MDA_stored', 'MEDX_stored', 'MLN_stored', 'MGO_stored', 'MC_stored', 'MYC_stored', 'NAS_stored', 'NEU_stored', 'NEXO_stored', 'NOAH_stored', 'nCash_stored', 'NULS_stored', 'NMR_stored', 'OCEAN_stored', 'OCN_stored', 'ODE_stored', 'OMG_stored', 'RNT_stored', 'OSA_stored', 'PRL_stored', 'PAX_stored', 'PLR_stored', 'PNK_stored', 'PITCH_stored', 'POE_stored', 'POLY_stored', 'PETH_stored', 'PPT_stored', 'POWR_stored', 'PRO_stored', 'PMA_stored', 'NPXS_stored', 'QASH_stored', 'QRL_stored', 'QNT_stored', 'QSP_stored', 'QKC_stored', 'QBIT_stored', 'RDN_stored', 'REN_stored', 'REQ_stored', 'RSR_stored', 'R_stored', 'RHOC_stored', 'RUFF_stored', 'SKB_stored', 'SALT_stored', 'SAN_stored', 'OST_stored', 'AGI_stored', 'SRN_stored', 'SPANK_stored', 'SXDT_stored', 'STACS_stored', 'EURS_stored', 'SNT_stored', 'STRX_stored', 'STORJ_stored', 'STORM_stored', 'SUB_stored', 'SWM_stored', 'ESH_stored', 'SDEX_stored', 'TEL_stored', 'PAY_stored', 'EURT_stored', 'USDT_stored', 'THETA_stored', 'TCT_stored', 'TEN_stored', 'TOMO_stored', 'TUSD_stored', 'TUSD_stored', 'UTT_stored', 'USDC_stored', 'UTK_stored', 'UTNP_stored', 'VEN_stored', 'VERI_stored', 'VRS_stored', 'VETH_stored', 'WTC_stored', 'WAX_stored', 'WIC_stored', 'WSKR_stored', 'WBTC_stored', 'XYO_stored', 'ZIL_stored', 'ZOM_stored', 'LRCv2_stored', 'txs', 'avg_tx_time', 'volume_eth', 'volume_GNO', 'volume_OWL', 'volume_DAI', 'volume_MGN', 'volume_WETH', 'volume_0xBTC', 'volume_ZRX', 'volume_ADX', 'volume_ADT', 'volume_AE', 'volume_AION', 'volume_AST', 'volume_ALIS', 'volume_AMB', 'volume_ANT', 'volume_APIS', 'volume_REP', 'volume_AOA', 'volume_BNT', 'volume_BAT', 'volume_BAX', 'volume_BCAP', 'volume_BZNT', 'volume_BHPC', 'volume_BSOV', 'volume_BCT', 'volume_BQX', 'volume_BIX', 'volume_BKN', 'volume_VEE', 'volume_BLZ', 'volume_BNB', 'volume_BOOTY', 'volume_BNTY', 'volume_BRD', 'volume_BRZC', 'volume_BCZERO', 'volume_BTM', 'volume_CAN', 'volume_CENNZ', 'volume_CHX', 'volume_CND', 'volume_cZRX', 'volume_cREP', 'volume_cBAT', 'volume_cDAI', 'volume_cETH', 'volume_cUSDC', 'volume_CTXC', 'volume_CS', 'volume_CREDO', 'volume_CRO', 'volume_CPT', 'volume_C20', 'volume_AUTO', 'volume_CVC', 'volume_CMT', 'volume_GEN', 'volume_DPP', 'volume_DATA', 'volume_XD', 'volume_DAY', 'volume_MANA', 'volume_DENT', 'volume_DCN', 'volume_DESH', 'volume_DGTX', 'volume_DGD', 'volume_DNT', 'volume_DMT', 'volume_DRGN', 'volume_EDG', 'volume_LEND', 'volume_EDO', 'volume_ELF', 'volume_EDR', 'volume_ENG', 'volume_ENJ', 'volume_ERC20', 'volume_XET', 'volume_ETHS', 'volume_NEC', 'volume_ETHPLO', 'volume_FOAM', 'volume_FUN', 'volume_FSN', 'volume_GUSD', 'volume_GNX', 'volume_GVT', 'volume_GTO', 'volume_GIM', 'volume_GNT', 'volume_GRID', 'volume_GSE', 'volume_HOT', 'volume_HPB', 'volume_HT', 'volume_HYPE', 'volume_ICN', 'volume_ICX', 'volume_IDEX', 'volume_RLC', 'volume_INS', 'volume_INB', 'volume_IOST', 'volume_IOTX', 'volume_JNT', 'volume_KIN', 'volume_KNC', 'volume_LINK (Chainlink)', 'volume_LKY', 'volume_LPT', 'volume_LOOM', 'volume_LRC', 'volume_LRC', 'volume_MFT', 'volume_MKR', 'volume_MAN', 'volume_MXM', 'volume_MCO', 'volume_MDA', 'volume_MEDX', 'volume_MLN', 'volume_MGO', 'volume_MC', 'volume_MYC', 'volume_NAS', 'volume_NEU', 'volume_NEXO', 'volume_NOAH', 'volume_nCash', 'volume_NULS', 'volume_NMR', 'volume_OCEAN', 'volume_OCN', 'volume_ODE', 'volume_OMG', 'volume_RNT', 'volume_OSA', 'volume_PRL', 'volume_PAX', 'volume_PLR', 'volume_PNK', 'volume_PITCH', 'volume_POE', 'volume_POLY', 'volume_PETH', 'volume_PPT', 'volume_POWR', 'volume_PRO', 'volume_PMA', 'volume_NPXS', 'volume_QASH', 'volume_QRL', 'volume_QNT', 'volume_QSP', 'volume_QKC', 'volume_QBIT', 'volume_RDN', 'volume_REN', 'volume_REQ', 'volume_RSR', 'volume_R', 'volume_RHOC', 'volume_RUFF', 'volume_SKB', 'volume_SALT', 'volume_SAN', 'volume_OST', 'volume_AGI', 'volume_SRN', 'volume_SPANK', 'volume_SXDT', 'volume_STACS', 'volume_EURS', 'volume_SNT', 'volume_STRX', 'volume_STORJ', 'volume_STORM', 'volume_SUB', 'volume_SWM', 'volume_ESH', 'volume_SDEX', 'volume_TEL', 'volume_PAY', 'volume_EURT', 'volume_USDT', 'volume_THETA', 'volume_TCT', 'volume_TEN', 'volume_TOMO', 'volume_TUSD', 'volume_TUSD', 'volume_UTT', 'volume_USDC', 'volume_UTK', 'volume_UTNP', 'volume_VEN', 'volume_VERI', 'volume_VRS', 'volume_VETH', 'volume_WTC', 'volume_WAX', 'volume_WIC', 'volume_WSKR', 'volume_WBTC', 'volume_XYO', 'volume_ZIL', 'volume_ZOM', 'volume_LRCv2', 'payment_eth_txs', 'payment_OWL_txs', 'payment_DAI_txs', 'payment_WETH_txs', 'payment_KNC_txs', 'payment_MKR_txs', 'payment_RDN_txs']

    unknown_columns = []
    for column in columns:
        if column not in known_columns:
            unknown_columns.append(column)
    if len(unknown_columns) > 0:
        print("Columns changed!")
        print("New columns: {}".format(unknown_columns))
        import pdb; pdb.set_trace()
    
    stats_file = open(STATS_FILENAME, 'w')
    stats_file.write(','.join(columns))
    stats_file.write('\n')

    for data in stats.allData():
        stats_file.write(','.join([str(d) for d in data]))
        stats_file.write('\n')
        stats_file.flush()
        
    stats_file.close()

if __name__ == '__main__':
    main()

