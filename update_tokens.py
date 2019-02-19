import requests
import json

SAFE_RELAY = 'https://safe-relay.gnosis.pm'

def main():

    # Get the tokens in tokens.csv
    tokens = {}

    with open('tokens.csv', 'r') as f:
        for line in f.readlines():
            token = json.loads(line)
            tokens[token['address']] = token

    num_tokens = len(tokens)

    # Fetch token info from server and update / add to tokens
    next_url = '{}/api/v1/tokens/?limit=100'.format(SAFE_RELAY) 
    while next_url:
        response = requests.get(next_url)
        data = response.json()
        next_url = data['next']

        for new_token in data['results']:
            tokens[new_token['address']] = new_token
        
    # Write back to token.csv
    with open('tokens.csv', 'w') as f:
        for token_address in tokens:
            token_info = tokens[token_address]
            f.write('{}\n'.format(json.dumps(token_info)))

    print('Added {} new tokens to tokens.csv. Has now {} tokens.'.format(len(tokens) - num_tokens, len(tokens)))

if __name__ == '__main__':
    main()
