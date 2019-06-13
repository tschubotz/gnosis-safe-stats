from web3 import Web3
import json
import requests
import os
from dateutil import parser as date_parser


SAFE_RELAY_USERNAME = ''
SAFE_RELAY_PASSWORD = ''
STATS_FILENAME = 'stats.csv'

def main():
    if os.path.exists(STATS_FILENAME):
        print('{} exists. Please delete or choose different filename.'.format(STATS_FILENAME))
        exit(0)

    # STEP 1: Get Safes from backend
    print('STEP 1: Get Safes from backend')

    # Auth
    auth_data = {
        'username': SAFE_RELAY_USERNAME, 
        'password': SAFE_RELAY_PASSWORD
        }
    auth_headers = {
        'Content-Type': 'application/json'
        }
    auth_request = requests.post("https://safe-relay.gnosis.pm/api/v1/private/api-token-auth/", headers=auth_headers, data=json.dumps(auth_data))
    auth_token = auth_request.json().get('token')

    get_safes_headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Token {}'.format(auth_token)
        }

    safes = []
    get_safes_next = 'https://safe-relay.gnosis.pm/api/v1/private/safes/?limit=100'
    while get_safes_next:
        get_safes_request = requests.get(get_safes_next, headers=get_safes_headers)
        get_safes_json = get_safes_request.json()
        print('\tGot batch of {} safes from backend'.format(len(get_safes_json.get('results'))))
        
        for get_safe in get_safes_json.get('results'):
            safe = get_safe.copy()
            safe['address'] = Web3.toChecksumAddress(safe['address'])
            safe['tokensWithBalance'] = {}
            for token in get_safe['tokensWithBalance']:
                safe['tokensWithBalance'][token['tokenAddress']] = token
            safes.append(safe)

        get_safes_next = get_safes_json.get('next')
        
    print('Got {} safes in total'.format(len(safes)))

    # STEP 2: Get tokens from backend
    print('STEP 2: Get tokens from backend')
    tokens = {}
    get_tokens_next = 'https://safe-relay.gnosis.pm/api/v1/tokens/?limit=100'
    while get_tokens_next:
        get_tokens_request = requests.get(get_tokens_next)
        get_tokens_json = get_tokens_request.json()
        
        print('\tGot batch of {} tokens from backend'.format(len(get_tokens_json.get('results'))))
        get_tokens_next = get_tokens_json.get('next')

        for new_token in get_tokens_json['results']:
            tokens[new_token['address']] = new_token

    print('Got {} tokens in total'.format(len(tokens)))  

    # Get token symbols for header row
    column_names = ['safe_address', 'created_at', 'version', 'nonce', 'num_owners', 'threshold', 'ETH']
    for token_address in sorted(tokens):
        column_names.append(tokens[token_address]['symbol'])

    stats_file = open(STATS_FILENAME, 'w')
    stats_file.write(','.join(column_names))
    stats_file.write('\n')
    
    # STEP 3: Go through safes, get details and write to output file
    print('STEP 3: Go through safes, get details and write to output file')
    for i,safe in enumerate(safes):
        print('\tGet info for Safe {}/{}'.format(i + 1, len(safes)))

        get_safe_info_request = requests.get('https://safe-relay.gnosis.pm/api/v1/safes/{}'.format(safe['address']))
        get_safe_info_json = get_safe_info_request.json()
        
        owners = get_safe_info_json.get('owners')
        num_owners = len(owners) if owners else 0

        creation_datetime = date_parser.parse(safe['created'])

        eth_balance = safe['balance'] if safe['balance'] else 0

        out = [
            safe['address'],
            str(creation_datetime.date()),
            get_safe_info_json.get('version'),
            get_safe_info_json.get('nonce'),
            num_owners,
            get_safe_info_json.get('threshold'),
            eth_balance / 10**18,
        ]

        for token in sorted(tokens):
            if token in safe['tokensWithBalance']:
                decimals = tokens[token]['decimals']
                balance = safe['tokensWithBalance'][token]['balance']
                out.append(balance / 10 ** decimals)
            else:
                out.append(0)
        
        stats_file.write(','.join(map(str, out)))
        stats_file.write('\n')
        stats_file.flush()

        
    stats_file.close()

if __name__ == '__main__':
    main()

