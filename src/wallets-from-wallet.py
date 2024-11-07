import requests

from dotenv import load_dotenv
import os

def get_block_number(timestamp):
    url = f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={timestamp}&closest=before&apikey={os.getenv('ETHERSCAN_API_KEY')}'
    response = requests.get(url)
    return response.json()['result']

def get_wallet_tx_list(wallet, startblock, endblock):
    url = f'https://api.etherscan.io/api?module=account&action=txlist&address={wallet}&startblock={startblock}&endblock={endblock}&sort=asc&apikey={os.getenv('ETHERSCAN_API_KEY')}'
    response = requests.get(url)
    return response.json()['result']

def get_maker_addresses_from_specified_address():
    timestamp_from = int(input('Enter timestamp from: '))
    timestamp_to = int(input('Enter timestamp to: '))
    wallet = input('Enter wallet: ')

    startblock = get_block_number(timestamp_from)
    endblock = get_block_number(timestamp_to)
    tx_list = get_wallet_tx_list(wallet, startblock, endblock)

    unique_makers = list(set(tx['from'] for tx in tx_list))

    csv_string = '\n'.join(unique_makers)
    file_path = f'{wallet}-{timestamp_from}-{timestamp_to}.csv'

    with open(file_path, 'w') as file:
        file.write(csv_string)

    print('CSV file saved successfully.')

get_maker_addresses_from_specified_address()
