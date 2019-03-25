from requests_html import HTMLSession
import json
import csv

SAFE_CONTRACT = '0x567725581c7518D86c7d163Dd579b2c4258337d0'  # Just a random Safe proxy contract to look out for online. Not checking the mastercopy or something yet.

def main():

    # Get the Safes in safes.csv
    safes = set()

    with open('safes.csv', 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for line in reader:
            address = line[0]
            safes.add(address)

    num_safes = len(safes)
    
    # Fetch Safe addresses from https://etherscan.io/find-similar-contracts?a=<address>
    session = HTMLSession()
    url = 'https://etherscan.io/find-similar-contracts?a={}'.format(SAFE_CONTRACT)
    response = session.get(url)

    if not response.ok or 'unexpected error' in response.text:
        print('Error loading Safe addresses from Etherscan. Does this link work?\n{}'.format(url))
    
    # Get the Safes addresses #hacky
    for td in response.html.find('td'):
        text = td.text
        if text[:2] == '0x':  # I know, very robust
            safes.add(text)
    
    # Write back to safes.csv
    with open('safes.csv', 'w') as f:
        for safe_address in safes:
            f.write('{}\n'.format(safe_address))

    print('Added {} new Safe addresses to safes.csv. Has now {} Safe addresses.'.format(len(safes) - num_safes, len(safes)))

if __name__ == '__main__':
    main()
