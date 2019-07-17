from web3 import Web3
import json
import requests
import os
from dateutil import parser as date_parser
import datetime

STATS_FILENAME = ''

TOKENS_TO_BE_ADDED = [
    '0x0000000000085d4780B73119b644AE5ecd22b376',  # TUSD
    '0x01b3Ec4aAe1B8729529BEB4965F27d008788B0EB',   # DPP
    '0x175666d12fC722aBD9E4a8EbF5d9B2d17b36B268',  # WSKR
    '0x1d462414fe14cf489c7A21CaC78509f4bF8CD7c0',  # CAN
    '0xEA610B1153477720748DC13ED378003941d84fAB',  # ALIS
    '0xE814aeE960a85208C3dB542C53E7D4a6C8D5f60F', # DAY
    '0xc3761EB917CD790B30dAD99f6Cc5b4Ff93C4F9eA',  #ERC20
    '0xB705268213D593B8FD88d3FDEFF93AFF5CbDcfAE', #IDEX
    '0x6B01c3170ae1EFEBEe1a3159172CB3F7A5ECf9E5',  #BOOTY
    '0x42d6622deCe394b54999Fbd73D108123806f6a18',  # SPANK
]
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
    '0x552d72f86f04098a4eaeDA6D7b665AC12f846AD2'
]

FORMER_PAYMENT_TOKENS = [
    '0x255Aa6DF07540Cb5d3d297f0D0D4D84cb52bc8e6'
]

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

def main():
    if os.path.exists(STATS_FILENAME):
        print('{} exists. Please delete or choose different filename.'.format(STATS_FILENAME))
        exit(0)

    # Get payment tokens from backend
    tokens = {}
    payment_tokens = {}
    token_response = requests.get('https://safe-relay.gnosis.pm/api/v1/tokens/?limit=200')
    for token in token_response.json().get('results'):
        tokens[token['address']] = token
        if token['gas'] or token['address'] in FORMER_PAYMENT_TOKENS:
            payment_tokens[token['address']] = token
    
    # Prepare data structure
    data = {}
    start_date = datetime.date(2018, 1, 1)
    end_date = datetime.date.today() + datetime.timedelta(days=1)
    for date in daterange(start_date, end_date):
        data[str(date)] = {
            'median_execution_time': '',
            'volume_eth': 0
        }
        # Add token volume columns
        for _, token_info in tokens.items():
            # LRC is 2 times in our db #hacky
            if token_info['address'] == '0xBBbbCA6A901c926F240b89EacB641d8Aec7AEafD':
                data[str(date)]['volume_{}v2'.format(token_info['symbol'])] = 0    
            else:
                data[str(date)]['volume_{}'.format(token_info['symbol'])] = 0

        # Add payment token columns
        data[str(date)]['payment_eth_num_tx'] = 0
        for _, token_info in payment_tokens.items():
            data[str(date)]['payment_{}_num_tx'.format(token_info['symbol'])] = 0
        

    # Get history stats
    response = requests.get('https://safe-relay.gnosis.pm/api/v1/stats/history/?fromDate={}'.format(start_date))
    
    # Median transaction times
    median_execution_times = response.json().get('relayedTxs').get('averageExecutionTimeSeconds')
    for item in median_execution_times:
        data[item['createdDate']]['median_execution_time'] = float(item['median'])


    # Ether volume
    ether_volume = response.json().get('relayedTxs').get('volume').get('ether')
    for item in ether_volume:
        data[item['date']]['volume_eth'] = int(item['value']) / (10**18)

    # Token volumes
    token_volumes = response.json().get('relayedTxs').get('volume').get('tokens')
    for item in token_volumes:
        token_address = item['tokenAddress']
        token = tokens.get(token_address, None)
        if not token:
            if token_address not in TOKENS_TO_BE_ADDED and token_address not in TOKENS_TO_IGNORE:
                print('Could not find info for token at https://etherscan.io/address/{}'.format(item['tokenAddress']))
            continue
        # Special handling for LRC since it exists in 2 versions. #hacky.
        if token_address == '0xBBbbCA6A901c926F240b89EacB641d8Aec7AEafD':
            data[item['date']]['volume_{}v2'.format(token['symbol'])] = int(item['value']) / (10**int(token['decimals']))
        else:
            data[item['date']]['volume_{}'.format(token['symbol'])] = int(item['value']) / (10**int(token['decimals']))

    # Payment tokens
    payment_token_num_txs = response.json().get('relayedTxs').get('paymentTokens')
    for item in payment_token_num_txs:
        gas_token_address = item['gasToken']
        if gas_token_address is None:
            data[item['date']]['payment_eth_num_tx'] = item['number']
        elif gas_token_address not in payment_tokens:
            print('Ignored payment token: https://etherscan.io/address/{}'.format(gas_token_address))
            continue
        else:
            token = payment_tokens.get(gas_token_address)
            data[item['date']]['payment_{}_num_tx'.format(token['symbol'])] = item['number']


    # Write output
    columns = ['date', 'median_execution_time', 'volume_eth']
    for _, token_info in tokens.items():
        # Special handling for LRC since it exists in 2 versions. #hacky.
        if token_info['address'] == '0xBBbbCA6A901c926F240b89EacB641d8Aec7AEafD':
            columns.append('volume_{}v2'.format(token_info['symbol']))
        else:
            columns.append('volume_{}'.format(token_info['symbol']))
    columns.append('payment_eth_num_tx')
    for _, token_info in payment_tokens.items():
        columns.append('payment_{}_num_tx'.format(token_info['symbol']))

    stats_file = open(STATS_FILENAME, 'w')
    stats_file.write(','.join(columns))
    stats_file.write('\n')

    for date, values in data.items():
        column_values = [date]

        for _, column_value in values.items():
            column_values.append(str(column_value))

        stats_file.write(','.join(column_values))
        stats_file.write('\n')
        stats_file.flush()
        
    stats_file.close()

if __name__ == '__main__':
    main()

