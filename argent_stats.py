from web3.exceptions import BadFunctionCallOutput
from web3 import Web3, HTTPProvider
import csv
from multiprocessing import Pool
import json
import sha3
import requests
from gnosis.eth.contracts import get_erc20_contract

INFURA_API_KEY = ''
NUM_PROCESSES = 128
WALLET_FACTORY_ADDRESS = '0x851cC731ce1613AE4FD8EC7F61F4B350F9CE1020'
WALLET_FACTORY_ABI = '''
[{"constant":false,"inputs":[{"name":"_moduleRegistry","type":"address"}],"name":"changeModuleRegistry","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"getENSReverseRegistrar","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_wallet","type":"address"}],"name":"init","outputs":[],"payable":false,"stateMutability":"pure","type":"function"},{"constant":false,"inputs":[{"name":"_manager","type":"address"}],"name":"addManager","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"getENSRegistry","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_manager","type":"address"}],"name":"revokeManager","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"ensManager","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"ADDR_REVERSE_NODE","outputs":[{"name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"walletImplementation","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_ensManager","type":"address"}],"name":"changeENSManager","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_node","type":"bytes32"}],"name":"resolveEns","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_newOwner","type":"address"}],"name":"changeOwner","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"ensResolver","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_owner","type":"address"},{"name":"_modules","type":"address[]"},{"name":"_label","type":"string"}],"name":"createWallet","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"moduleRegistry","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_walletImplementation","type":"address"}],"name":"changeWalletImplementation","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_ensResolver","type":"address"}],"name":"changeENSResolver","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"managers","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"inputs":[{"name":"_ensRegistry","type":"address"},{"name":"_moduleRegistry","type":"address"},{"name":"_walletImplementation","type":"address"},{"name":"_ensManager","type":"address"},{"name":"_ensResolver","type":"address"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"name":"addr","type":"address"}],"name":"ModuleRegistryChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"addr","type":"address"}],"name":"WalletImplementationChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"addr","type":"address"}],"name":"ENSManagerChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"addr","type":"address"}],"name":"ENSResolverChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_wallet","type":"address"},{"indexed":true,"name":"_owner","type":"address"}],"name":"WalletCreated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_manager","type":"address"}],"name":"ManagerAdded","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_manager","type":"address"}],"name":"ManagerRevoked","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_newOwner","type":"address"}],"name":"OwnerChanged","type":"event"}]
'''
WALLET_ABI = '''
[{"constant":false,"inputs":[{"name":"_newOwner","type":"address"}],"name":"setOwner","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_module","type":"address"},{"name":"_method","type":"bytes4"}],"name":"enableStaticCall","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_module","type":"address"},{"name":"_value","type":"bool"}],"name":"authoriseModule","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_owner","type":"address"},{"name":"_modules","type":"address[]"}],"name":"init","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"implementation","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"bytes4"}],"name":"enabled","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_target","type":"address"},{"name":"_value","type":"uint256"},{"name":"_data","type":"bytes"}],"name":"invoke","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"authorised","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"modules","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"module","type":"address"},{"indexed":false,"name":"value","type":"bool"}],"name":"AuthorisedModule","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"module","type":"address"},{"indexed":true,"name":"method","type":"bytes4"}],"name":"EnabledStaticCall","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"module","type":"address"},{"indexed":true,"name":"target","type":"address"},{"indexed":true,"name":"value","type":"uint256"},{"indexed":false,"name":"data","type":"bytes"}],"name":"Invoked","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"value","type":"uint256"},{"indexed":true,"name":"sender","type":"address"},{"indexed":false,"name":"data","type":"bytes"}],"name":"Received","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"owner","type":"address"}],"name":"OwnerChanged","type":"event"}]
'''



def get_info_for_address(param, web3=None):
    address = param['safe_address']
    tokens = param['tokens']
    
    if not web3:
        web3 = Web3(HTTPProvider('https://mainnet.infura.io/v3/' + INFURA_API_KEY))
    
    output = [address]

    # Check if the contract exists
    if (web3.eth.getCode(address) == 0 or web3.eth.getCode(address).hex() in ('0x00', '0x', '0x0')):
        return

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

    # wallet_factory_contract = web3.eth.contract(WALLET_FACTORY_ADDRESS, abi=WALLET_FACTORY_ABI)
    
    wallet_created_topic_hash =  sha3.keccak_256()
    wallet_created_topic_hash.update('WalletCreated(address,address)'.encode('utf-8'))
    wallet_created_topic_hash = '0x{}'.format(wallet_created_topic_hash.hexdigest())

    logs = web3.eth.getLogs({'fromBlock': 7173577 , 'toBlock': 'latest', 'address': WALLET_FACTORY_ADDRESS, 'topics':[wallet_created_topic_hash]})
    
    wallet_addresses = []


    for log in logs:
        wallet_address = web3.toChecksumAddress('0x{}'.format(log['topics'][1].hex()[26:]))
        wallet_addresses.append(wallet_address)



    tokens = {}
    token_response = requests.get('https://safe-relay.gnosis.pm/api/v1/tokens/?limit=200')
    for token in token_response.json().get('results'):
        if token['symbol'] in ['GNO', 'WETH', 'DAI', 'OWL', 'OMG', 'ENJ', 'cDAI', 'ZRX', 'SNT', 'BAT', 'cETH', 'cBAT', 'cUSD', 'cWBTC', 'cREP', 'cZRX', 'PETH']:
            tokens[token['address']] = token

    # Get token symbols for header row
    column_names = ['wallet_address', 'ETH']
    for token_address in sorted(tokens):
        column_names.append(tokens[token_address]['symbol'])

    # Print header row
    print(','.join(column_names))
    
    # Prepare params
    params = []
    for address in wallet_addresses:
        param = {'safe_address': address, 'tokens': tokens}
        params.append(param)

    # get_info_for_address(params[1])

    # Do the work
    with Pool(processes=NUM_PROCESSES) as pool:
        pool.map(get_info_for_address, params)

if __name__ == '__main__':
    main()