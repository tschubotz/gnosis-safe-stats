from web3.exceptions import BadFunctionCallOutput
from web3 import Web3, HTTPProvider
import csv
from gnosis.eth.contracts import get_safe_contract, get_erc20_contract
from multiprocessing import Pool
import json

NUM_PROCESSES = 64
INFURA_API_KEY = ''


def get_info_for_address(param, web3=None):
    address = param['safe_address']
    tokens = param['tokens']
    
    if not web3:
        web3 = Web3(HTTPProvider('https://mainnet.infura.io/v3/' + INFURA_API_KEY))

    contract = get_safe_contract(w3=web3, address=address)

    output = [address]

    # Get nonce
    nonce = contract.functions.nonce().call()
    output.append(nonce)

    # Get number of owners
    owners = contract.functions.getOwners().call()
    output.append(len(owners))

    # Get threshold
    threshold = contract.functions.getThreshold().call()
    output.append(threshold)

    # Get ETH balance
    eth_balance = Web3.fromWei(web3.eth.getBalance(address), 'ether')
    output.append(eth_balance)

    # Get token balance for all tokens
    for token_address in sorted(tokens):
        token = tokens[token_address]
        
        contract = get_erc20_contract(w3=web3, address=token_address)
        try:
            balance = contract.functions.balanceOf(address).call() / (10**token['decimals'])
        except BadFunctionCallOutput:
            balance = 0
        output.append(balance)
    
    # Print result
    print(','.join(map(str, output)))
    


def main():
    web3 = Web3(HTTPProvider('https://mainnet.infura.io/v3/' + INFURA_API_KEY))

    # Get Safe addressses form safe.csv
    safe_addresses = []
    with open('safes.csv', 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for line in reader:
            address = line[0]
            safe_addresses.append(web3.toChecksumAddress(address))

    # Get tokens from tokens.csv
    tokens = {}
    with open('tokens.csv', 'r') as f:
        for line in f.readlines():
            token = json.loads(line)
            tokens[token['address']] = token

    # Get token symbols for header row
    column_names = ['safe_address', 'nonce', 'num_owners', 'threshold', 'ETH']
    for token_address in sorted(tokens):
        column_names.append(tokens[token_address]['symbol'])

    # Print header row
    print(','.join(column_names))
    
    # Prepare params
    params = []
    for address in safe_addresses:
        param = {'safe_address': address, 'tokens': tokens}
        params.append(param)

    get_info_for_address(params[0])
    
    # Do the work
    with Pool(processes=NUM_PROCESSES) as pool:
        pool.map(get_info_for_address, params)

if __name__ == '__main__':
    main()

