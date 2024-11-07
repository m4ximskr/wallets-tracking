import requests

from dotenv import load_dotenv
import os

networksMap = {
    'eth': 1,
    'arb': 42161,
}

def get_token_events(token_pair_contract_address, network_id, timestamp_from, timestamp_to):
    query = '''
    {
        getTokenEvents(
            limit: 1000
            query: {
                address: "%s",
                networkId: %d,
                eventDisplayType: [Buy, Sell],
                timestamp: {
                    from: %d,
                    to: %d
                }
            }
        ) {
            items {
                eventDisplayType
                maker
            }
        }
    }
    ''' % (token_pair_contract_address, network_id, timestamp_from, timestamp_to)

    response = requests.post(
        'https://graph.defined.fi/graphql',
        json={'query': query},
        headers={
            'Authorization': os.getenv('DEFINED_API_KEY')
        }
    )

    print(response.json())

    return response.json()['data']['getTokenEvents']['items']


def get_makers_addresses_from_token_pair_contract_address():
    token_pair_contract_address = input('Enter token pair contract address: ')
    network = input('Enter network (eth/arb): ')
    timestamp_from = int(input('Enter timestamp from: '))
    timestamp_to = int(input('Enter timestamp to: '))

    network_id = networksMap[network]

    token_events = get_token_events(token_pair_contract_address, network_id, timestamp_from, timestamp_to)
    unique_makers = list(set(event['maker'] for event in token_events))

    csv_string = '\n'.join(unique_makers)
    file_path = f"{token_pair_contract_address}-{timestamp_from}-{timestamp_to}.csv"

    with open(file_path, 'w') as file:
        file.write(csv_string)

    print('CSV file saved successfully.')


get_makers_addresses_from_token_pair_contract_address()
