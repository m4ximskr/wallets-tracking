import requests
import pprint
import csv
from datetime import datetime, timedelta

from dotenv import load_dotenv
import os

load_dotenv()

zerionHeaders = {
            "accept": "application/json",
            "authorization": "Basic " + os.getenv('ZERION_KEY')
        }

dexScreenerHeaders = {
            "accept": "application/json"
        }

networksMap = {
    'ethereum': 1,
    'arbitrum': 42161,
    'avalanche': 43114,
    'polygon': 137,
    'optimism': 10,
    'fantom': 250,
    'base': 8453,
}

allowedSymbols = ['WETH', 'ETH', 'USDT', 'USDC']
wallets = {}
transactions = 0
# Liquidity in $ that the token should match or exceed
# Calculated for all buys
# LIMITATION: Calculated at the moment of script execution
desiredLiquidity = 10000
rewardHash = '0x0000000000000000000000000000000000000000'


def sendRequest(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        # Handle connection errors or invalid URLs
        print(f"Request error: {e}")
    except requests.exceptions.HTTPError as e:
        # Handle HTTP errors (status codes >= 400)
        print(f"HTTP error: {e}")
    except requests.exceptions.Timeout as e:
        # Handle timeout errors
        print(f"Timeout error: {e}")
    except requests.exceptions.TooManyRedirects as e:
        # Handle too many redirects error
        print(f"Too many redirects error: {e}")
    except ValueError as e:
        # Handle JSON decoding errors
        print(f"JSON decoding error: {e}")

    return {
        'links': {},
        'data': [],
    }
def requestData(wallet):
    MAX_TRANSACTIONS = 100
    transactionCounter = 0
    pageSize = min(int(transactions), MAX_TRANSACTIONS)

    url = "https://api.zerion.io/v1/wallets/" + wallet + "/transactions/?currency=usd&page[size]=" + str(pageSize) + "&filter[operation_types]=trade&filter[status]=confirmed&filter[asset_types]=fungible"
    json_response = []
    transactionData = []

    print('transactionCounter', transactionCounter)
    print('int(transactions)', int(transactions))

    while transactionCounter < int(transactions):
        if transactionData:
            if 'next' in json_response['links']:
                url = json_response['links']['next']
            else:
                break
        json_response = sendRequest(url, zerionHeaders)
        print('JSON RESPONSE', json_response)
        transactionData.extend(json_response['data'])
        transactionCounter += pageSize

    pp = pprint.PrettyPrinter(indent=2)
    # pp.pprint(transactionData)
    print('TOTAL TRANSACTIONS: ', transactionCounter)

    return calculate(transactionData)

# Used to return symbol data from transfer of desired direction
def getPurchasedTokenData(transfers, transferDirection, chain):
    tokenData = {}

    for transfer in transfers:
        direction = transfer['direction']

        # Filter out rewards tokens that have $0 value (e.g https://etherscan.io/tx/0x3ead34a830f19974bb7ab67e705ae3db4ef0ab15acd06e020cc2de986296307f)
        if direction == transferDirection and transfer['recipient'] != rewardHash and transfer['sender'] != rewardHash:
            symbol = transfer['fungible_info']['symbol']
            value = transfer['value']  # USD value
            quantity = transfer['quantity']['float']  # No. of tokens purchased
            price = transfer['price']  # USD price per 1 token

            tokenAddressByChain = next(implementation['address'] for implementation in transfer['fungible_info']['implementations'] if implementation['chain_id'] == chain)

            # Checking for price/value = null as it can be N/A from Zerion
            # Using opposite price as it is includes fees paid (when selling) or uses initial sum (when buying)

            innerSymbol = ''
            innerValue = 0
            innerQuantity = 0

            for innerTransfer in transfers:
                direction = innerTransfer['direction']

                # Look for opposite transfer direction to get ETH values
                if direction != transferDirection:
                    innerSymbol = innerTransfer['fungible_info']['symbol']
                    innerValue = innerTransfer['value']  # USD value

                    # Accumulate qty as there can be several tokens
                    # in 1 transaction with None price and we only
                    # want to calculate price for total qty and not
                    # separately as it will cause the token price to
                    # look higher than it is
                else:
                    innerQuantity += float(innerTransfer['quantity']['numeric'])

            # If the price is unknown calculate it using ETH value/token qty
            # If the price of another token is null as well, leave price = None so that the transaction wouldn't
            # be tracked
            if innerSymbol in allowedSymbols and innerValue is not None:
                calculatedTokenPrice = innerValue / innerQuantity
                print('Calculated price ', calculatedTokenPrice)
                value = innerValue
                price = calculatedTokenPrice

            # https://api.zerion.io/v1/wallets/0xba7fd2b3913691b77c2a5bc413efa306b5a77fb9/transactions/?currency=usd&filter%5Basset_types%5D=fungible&filter%5Boperation_types%5D=trade&page%5Bsize%5D=100
            # 2023-05-01T11:30:12Z
            if symbol == '' or symbol != tokenData.get('symbol', ''):
                tokenData['symbol'] = symbol
                tokenData['chain'] = chain
                tokenData['address'] = tokenAddressByChain
                tokenData['data'] = {
                    'value': value,
                    'quantity': quantity,
                    'price': price
                }
            # Used for cases when there are 2 same assets in 1 transaction
            else:
                tokenData['data'] = {
                    'value': tokenData['data']['value'] + value,
                    'quantity': tokenData['data']['quantity'] + quantity,
                    'price': price
                }


    return tokenData

# Get PL, winrate, transaction count, % gain or loss by token, liquidity
def getTokenStats(transactionMap):
    tokenStats = {}

    for token in transactionMap:
        tokenAddress = transactionMap[token].get('address', '')

        # Can only handle ethereum/arbitrum/avalanche/polygon/optimism/base/fantom chains for now
        if len(tokenAddress) > 0 and transactionMap[token]['chain'] in networksMap:
            winRate = 0
            percentageGainOrLossByToken = None

            # Check for division by 0 - there can be SELL without a buy, so transaction count will stay 0
            if transactionMap[token]['tokenTransactions'] > 0:
                winRate = transactionMap[token]['profitableTokenTransactions'] / transactionMap[token]['tokenTransactions'] * 100

            # Calculate % gain/loss by token
            # We added all % from token transactions, and now we divide it by number of transactions
            if 'percentageGainOrLossByToken' in transactionMap[token] and transactionMap[token]['percentageGainOrLossByToken'] is not None:
                percentageGainOrLossByToken = round((transactionMap[token]['percentageGainOrLossByToken'] / transactionMap[token]['tokenTransactions']), 2)

            chainID = networksMap[transactionMap[token]['chain']]

            firstBuyDatetimeObject = datetime.strptime(transactionMap[token]['buyDates'][0], "%Y-%m-%dT%H:%M:%SZ")
            historicalData = getTokenHistoricalData(tokenAddress, chainID, firstBuyDatetimeObject)

            firstBuyUnixTimestamp = int(firstBuyDatetimeObject.timestamp())

            tokenStats[tokenAddress] = {
                'symbol': token,
                'pl': round(transactionMap[token]['plByToken'], 2),
                'winRate': str(round(winRate, 2)) + '%',
                'transactionCount': transactionMap[token]['tokenTransactions'],
                'percentageGainOrLossByToken': percentageGainOrLossByToken,
                'chain': transactionMap[token]['chain'],
                'firstBuyUnixTimestamp': firstBuyUnixTimestamp,
                'liquidityOnBuy': historicalData['liquidity'],
                'dailyVolumeOnBuy': historicalData['dailyVolume'],
#                 'liquidity': getLiquidity(transactionMap[token]['address']) if tokenAddress else None
            }

    return tokenStats

def getTokenHistoricalData(tokenAddress, chainID, datetimeObject):
    try:
        timestamp = 0

        # If requested date is today, we have to explicitly set it yesterday as dex guru doesn't return data for today
        if datetimeObject.date() == datetime.now().date():
            yesterdayDateTimeObject = datetimeObject - timedelta(days=1)
            timestamp = int(yesterdayDateTimeObject.timestamp())
        else:
            timestamp = int(datetimeObject.timestamp())

        url = f'https://api.dev.dex.guru/v1/chain/{chainID}/tokens/{tokenAddress}/market/history?begin_timestamp={timestamp}&end_timestamp={timestamp}'
        print('url', url)
        headers = {
            'accept': 'application/json',
            'api-key': os.getenv('DEX_GURU_API_KEY'),
        }
        tokenData = sendRequest(url, headers)

        if tokenData['data']:
            print('getTokenHistoricalData DATA')
            return {
                'liquidity': tokenData['data'][0]['liquidity_usd'],
                'dailyVolume': tokenData['data'][0]['volume24h_usd'],
            }
        else:
            print('getTokenHistoricalData EMPTY')
            return {
               'liquidity': 0,
               'dailyVolume': 0,
            }
    except:
        print('getTokenHistoricalData ERROR')
        return {
            'liquidity': None,
            'dailyVolume': None,
        }

# Get overall % gain or loss for wallet
def getPercentageGainOrLoss(tokenStats):
    cummulativePercentages = 0
    validTransactions = 0

    for token in tokenStats:
        percentageGainOrLossByToken = tokenStats[token]['percentageGainOrLossByToken']

        if percentageGainOrLossByToken is not None:
            validTransactions += 1
            cummulativePercentages += percentageGainOrLossByToken

    if validTransactions != 0:
        return round((cummulativePercentages / validTransactions), 2)

    return None

# Iterates over all BUY tokens and computes % of how many tokens have liquidity >= desiredLiquidity
# % is calculated taking into account number of tokens that had liquidity not None
# E.g. if there are 10 tokens and only 5 have liquidity, then 5 tokens will be base for 100%, not 10 tokens
def getTokenCorrespondanceToLiquidity(tokenStats):
    validLiquidity = 0
    validTransactions = 0

    for token in tokenStats:
        liquidity = tokenStats[token]['liquidityOnBuy']

        if liquidity is not None:
            validTransactions += 1
            if liquidity >= desiredLiquidity:
                validLiquidity += 1

    if validTransactions != 0:
        return round((validLiquidity / validTransactions) * 100, 2)

    return None

def getTokenFieldAvgValue(tokenStats, fieldName):
    valueSum = 0
    validTransactions = 0

    for token in tokenStats:
        value = tokenStats[token][fieldName]

        if value is not None:
            valueSum += value
            validTransactions += 1

    if validTransactions != 0:
        return round(valueSum / validTransactions, 2)
    else:
        return None

def getAvgTimePassed(tokenStats):
    secondsSum = 0
    validTransactions = 0
    avgSeconds = 0

    for token in tokenStats:
        seconds = tokenStats[token]['secondsPassedFromCreationToFirstBuy']

        if seconds is not None:
            secondsSum += seconds
            validTransactions += 1

    if validTransactions != 0:
        avgSeconds = round(secondsSum / validTransactions)
    else:
        avgSeconds = 0

    return getSecondsToReadableFormat(avgSeconds)

def getSecondsToReadableFormat(seconds):
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    return f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"

def getPercentageOfTokenDescription(tokenStats):
    validDescriptions = 0
    validTransactions = 0

    for token in tokenStats:
        description = tokenStats[token]['description']

        if description is not None:
            validTransactions += 1
            if len(description) > 0:
                validDescriptions += 1

    if validTransactions != 0:
        return round((validDescriptions / validTransactions) * 100, 2)
    else:
        return None

def getPercentageOfScam(tokenStats):
    validScams = 0
    validTransactions = 0

    for token in tokenStats:
        isScam = tokenStats[token]['isScam']

        if isScam is not None:
            validTransactions += 1
            if isScam:
                validScams += 1

    if validTransactions != 0:
        return round((validScams / validTransactions) * 100, 2)
    else:
        return None


def calculate(data):
    print('----START---', data)

    if len(data) == 0:
        return

    transactionMap = {}
    profit = 0
    # Largest loss in $
    largestLoss = None

    # Largest gain in $
    largestGain = None

    # Largest number of consecutive transactions resulting in loss
    lossStreak = None
    lossCounter = 0

    # Cumulative buy volume - what $ amount is used for trades
    # Later it is divided by buyTransactionCount to get avg for wallet
    cumulativeBuyVolume = 0
    buyTransactionCount = 0

    profitableTransactionCount = 0
    pp = pprint.PrettyPrinter(indent=2)
    dateOfLastTransaction = data[0]['attributes']['mined_at']

    # Only sell with a buy in our dict
    totalTransactionCount = 0

    # Use reversed to iterate over oldest transactions first to first find BUY and then SELL
    for transaction in reversed(data):

        transactionDate = transaction['attributes']['mined_at']
        print('transactionDate', transactionDate)
        transfers = transaction['attributes']['transfers']
        chain = transaction['relationships']['chain']['data']['id']

        # Skip iteration if there is only one in/out transfer, meaning that transaction is broken and cant be used for calculation (by MAX)
        if len(transfers) == 1:
            continue

        for transfer in transfers:
            direction = transfer['direction']
            symbol = transfer['fungible_info']['symbol']
            price = transfer['value']

            # Buy transaction
            if direction == 'out':
                if symbol in allowedSymbols:
                    # cumulativeBuyVolume and buyTransactionCount used to calculate avg buy volume
                    # (what avg $ amount is used for trades)
                    if price is not None:
                        cumulativeBuyVolume += price
                        buyTransactionCount += 1

                    tokenData = getPurchasedTokenData(transfers, 'in', chain)

#                   https://api.zerion.io/v1/wallets/0xd25f3ff4d63179800dce837dc5412dac1ba6133f/transactions/?currency=usd&filter%5Basset_types%5D=fungible&filter%5Boperation_types%5D=trade&page%5Bsize%5D=100
#                   2023-10-05T01:47:53Z
#                   no transfers with 'in' direction, so tokenData is empty
                    if len(tokenData) == 0:
                        break

                    buySymbol = tokenData['symbol']

                    print('BUY: ', buySymbol, ', QTY: ', tokenData['data']['quantity'], ', FOR:', tokenData['data']['value'])
                    print('–--')

                    # There can be cases when there is no way to calculate the price,
                    # so we do not want to include these buy transactions
                    # E.g. https://etherscan.io/tx/0x3818d8ea5353da9c05a650aea996ba5d54e969bf26033cb988400837a8e65d45
                    #
                    # We are not doing anything with SELL as it won't be triggered without buy
                    if tokenData['data']['price'] is None:
                        break

                    if buySymbol not in transactionMap:
                        # Add new buy transaction to dict
                        transactionMap[buySymbol] = {
                            'transactions': [tokenData['data']],
                            'plByToken': 0,
                            'tokenTransactions': 0,
                            'profitableTokenTransactions': 0,
                            'address': tokenData['address'],  # Add address of pair to later calculate the liquidity
                            'chain': tokenData['chain'],
                            'buyDates': [transactionDate],
                            'sellDates': []
                        }
                    else:
                        # If symbol is already in dict update the array,
                        # otherwise use empty array to add this transaction
                        transactionMapTokenData = transactionMap[buySymbol]

                        tokenTransactions = transactionMapTokenData['transactions']
                        tokenTransactions.append(tokenData['data'])
                        transactionMapTokenData['transactions'] = tokenTransactions

                        tokenBuyDates = transactionMapTokenData['buyDates']
                        tokenBuyDates.append(transactionDate)
                        transactionMapTokenData['buyDates'] = tokenBuyDates

            # Sell transaction
            # ???Limitation - we are only looking at ETH
            if direction == 'in':
                if symbol in allowedSymbols:
                    tokenData = getPurchasedTokenData(transfers, 'out', chain)

#                   https://api.zerion.io/v1/wallets/0xceae4fcd03917d417389eb0fc96ebec1e89414d1/transactions/?currency=usd&filter%5Basset_types%5D=fungible&filter%5Boperation_types%5D=trade&page%5Bsize%5D=100
#                   2023-10-06T17:00:27Z
#                   no transfers with 'out' direction, so tokenData is empty
                    if len(tokenData) == 0:
                        break

                    sellSymbol = tokenData['symbol']
                    sellQuantity = tokenData['data']['quantity']
                    sellPrice = tokenData['data']['price']

                    if tokenData['data']['price'] is None:
                        break

                    # print('SELL: ', sellSymbol)
                    # pp.pprint(tokenData)
                    # print('---')

                    if sellSymbol not in transactionMap:
                        print('SELL transaction without a buy!')
                        print('SELL: ', sellSymbol, ', QTY: ', sellQuantity, ', FOR:', sellPrice * sellQuantity)
                        print('–--')
                    else:
                        tokenTransactions = transactionMap[sellSymbol]['transactions']
                        pl = 0
                        cummulativePercentageGainOrLossByToken = None

                        for idx, tokenTransaction in enumerate(tokenTransactions):
                            tokenPurchaseQty = tokenTransaction['quantity']
                            tokenPurchasePrice = tokenTransaction['price']

                            # There can be cases when SELL is based on 2 transaction values or SELL is smaller
                            # than 1 transactions value
                            qtyToDeduct = min(tokenPurchaseQty, sellQuantity)
                            transactionMap[sellSymbol]['transactions'][idx]['quantity'] = tokenPurchaseQty - qtyToDeduct

                            plByToken = qtyToDeduct * sellPrice - qtyToDeduct * tokenPurchasePrice

                            # Calculating % gain by token
                            if qtyToDeduct != 0 and sellPrice != 0:
                                calculatedPercentage = (qtyToDeduct * sellPrice - qtyToDeduct * tokenPurchasePrice) / (qtyToDeduct * tokenPurchasePrice) * 100

                                # Handling > 1 transaction with the same token inside 1 transaction
                                # We accumulate % because we can only get correct avg if we add everything and divide by total No of transactions
                                if cummulativePercentageGainOrLossByToken is not None:
                                    cummulativePercentageGainOrLossByToken = cummulativePercentageGainOrLossByToken + calculatedPercentage
                                else:
                                    cummulativePercentageGainOrLossByToken = calculatedPercentage

                            transactionMap[sellSymbol]['plByToken'] = transactionMap[sellSymbol]['plByToken'] + plByToken
                            pl += plByToken

                            if plByToken > 0:
                                transactionMap[sellSymbol]['profitableTokenTransactions'] = transactionMap[sellSymbol]['profitableTokenTransactions'] + 1
                            transactionMap[sellSymbol]['tokenTransactions'] = transactionMap[sellSymbol]['tokenTransactions'] + 1

                            print('SELL: ', sellSymbol, 'PL on transaction: ', plByToken)
                            print('–--')

                            # Reduce qty for next iteration as we already calculated PL for this
                            sellQuantity -= qtyToDeduct

                            # If we already calculated PL for whole qty sold, break from transaction loop
                            if sellQuantity == 0:
                                break

                        # Accumulating % gain/loss by token to calculate the average later
                        if 'percentageGainOrLossByToken' in transactionMap[sellSymbol]:
                            if cummulativePercentageGainOrLossByToken is not None and transactionMap[sellSymbol]['percentageGainOrLossByToken'] is not None:
                                transactionMap[sellSymbol]['percentageGainOrLossByToken'] = transactionMap[sellSymbol]['percentageGainOrLossByToken'] + cummulativePercentageGainOrLossByToken
                        else:
                            transactionMap[sellSymbol]['percentageGainOrLossByToken'] = cummulativePercentageGainOrLossByToken

                        # Setting the largest loss for the wallet
                        if largestLoss is not None:
                            largestLoss = min(largestLoss, pl)
                        elif pl < 0:
                            largestLoss = pl

                        # Setting the largest gain for the wallet
                        if largestGain is not None:
                            largestGain = max(largestGain, pl)
                        elif pl > 0:
                            largestGain = pl

                        if pl < 0:
                            lossCounter += 1

                            if lossStreak is not None:
                                lossStreak = max(lossCounter, lossStreak)
                            else:
                                lossStreak = lossCounter
                        else:
                            lossCounter = 0

                        profit += pl

                        if pl > 0:
                            profitableTransactionCount += 1
                        totalTransactionCount += 1

                        transactionMapTokenData = transactionMap[sellSymbol]

                        tokenSellDates = transactionMap[sellSymbol]['sellDates']
                        tokenSellDates.append(transactionDate)
                        transactionMapTokenData['sellDates'] = tokenSellDates

    if totalTransactionCount > 0:
        winRate = profitableTransactionCount/totalTransactionCount * 100
    else:
        winRate = 'No SELL transactions yet'

    avgBuyVolume = 0

    if buyTransactionCount != 0:
        avgBuyVolume = cumulativeBuyVolume / buyTransactionCount

    unrealizedStats = getUnrealizedStats(transactionMap)
    # If there are 0 profitable transactions and Defined couldn't find unrealized token prices, we want to avoid division by 0
    unrealizedWinRate = -1 if (totalTransactionCount + unrealizedStats['totalTransactions']) == 0 else (profitableTransactionCount + unrealizedStats['profitableTransactions'])/(totalTransactionCount + unrealizedStats['totalTransactions']) * 100
    unrealizedProfit = profit + unrealizedStats['pl']
    tokenStats = getTokenStats(transactionMap)
    updateTokenStatsWithAdditionalData(tokenStats)


    print('Profit: ', profit)
    print('Winrate: ', winRate)

    tokenAmount = len(tokenStats.keys())

    return {
                'profit': '$' + str(round(profit, 2)),
                'unrealizedProfit': '$' + str(round(unrealizedProfit, 2)),
                'winRate': winRate,
                'unrealizedWinRate': round(unrealizedWinRate, 2),
                'tokenStats': tokenStats,
                'totalTransactionCount': totalTransactionCount,
                'dateOfLastTransaction': dateOfLastTransaction,
                'percentageGainOrLoss': str(getPercentageGainOrLoss(tokenStats)) + '%',
                'largestLoss': '$' + str(largestLoss),
                'largestGain': '$' + str(largestGain),
                'lossStreak': lossStreak or 0,
                'avgBuyVolume': '$' + str(round(avgBuyVolume, 2)),
                'liquidityMatch': str(getTokenCorrespondanceToLiquidity(tokenStats)) + '%',
                'avgLiquidityOnBuy': getTokenFieldAvgValue(tokenStats, 'liquidityOnBuy'),
                'avgLiquidityCurrent': getTokenFieldAvgValue(tokenStats, 'liquidityCurrent'),
                'avgDailyVolumeOnBuy': getTokenFieldAvgValue(tokenStats, 'dailyVolumeOnBuy'),
                'avgDailyVolumeCurrent': getTokenFieldAvgValue(tokenStats, 'dailyVolumeCurrent'),
                'avgTimePassedFromCreationToFirstBuy': getAvgTimePassed(tokenStats),
                'tokenAmount': tokenAmount,
                'percentageOfDescription': getPercentageOfTokenDescription(tokenStats),
                'percentageOfScam': getPercentageOfScam(tokenStats),
            }

# Helper function to split a list into chunks
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

# Should be used for BUY transactions without sell
# Used for calculating unrealized profit
def getCurrentTokenPrices(addresses):
    chunk_size = 25
    address_prices_map = {}

    for chunk in chunk_list(addresses, chunk_size):
        inputs = ", ".join([f'{{ address: "{address}", networkId: 1 }}' for address in chunk])

        query = f'''
        {{
            getTokenPrices(
                inputs: [{inputs}],
            ) {{
                priceUsd
                address
            }}
        }}
        '''

        try:
            response = requests.post(
                'https://graph.defined.fi/graphql',
                json={'query': query},
                headers={
                    'Authorization': os.getenv('DEFINED_API_KEY')
                }
            )

            response.raise_for_status()  # Raise an error for non-2xx status codes

            data = response.json()
            token_prices = data.get('data', {}).get('getTokenPrices', [])

            if len(token_prices) > 0:
                for item in token_prices:
                    if item is not None:
                        address_prices_map[item.get('address')] = item.get('priceUsd', -1)
        except requests.exceptions.RequestException as e:
            # Handle connection errors or invalid HTTP responses
            print("Error:", e)
            for address in chunk:
                address_prices_map[address] = None

    return address_prices_map

# Here transactionMap will contain remaining tokens.
# Meaning that we have updated qty and other values for tokens that have buy-sell pair.
# If there is some token with qty >1 in this map, it means there is no pair for it
# and we have to calculate unrealized values.
def getUnrealizedStats(transactionMap):
    innerTransactionMap = {}
    tokensToQuery = []
    pl = 0
    profitableTransactions = 0
    totalTransactions = 0


    for token in transactionMap:
        if transactionMap[token]['transactions']:
            for transaction in transactionMap[token]['transactions']:
                # We do not care if there is some 0.00001 qty left
                if transaction['quantity'] >= 1:

                    # Add transactions to new array to calculate PL, winrate individually
                    if transactionMap[token]['address'] in innerTransactionMap:
                        innerTransactionMap[transactionMap[token]['address']]['transactions'].append(transaction)
                    else:
                        innerTransactionMap[transactionMap[token]['address']] = {
                            'transactions': [transaction]
                        }

                    tokensToQuery.append(transactionMap[token]['address'])
    uniqueValuesToQuery = list(set(tokensToQuery))
    livePrices = getCurrentTokenPrices(uniqueValuesToQuery)

    for address in innerTransactionMap:
        for transaction in innerTransactionMap[address]['transactions']:
            # There can be a case when Defined doesn't return a price
            if address in livePrices and livePrices[address] != -1:

                totalTransactions += 1
                oldValue = transaction['price'] * transaction['quantity']
                newValue = livePrices[address] * transaction['quantity']

                calculatedPl = newValue - oldValue
                pl += calculatedPl

                if calculatedPl > 0:
                    profitableTransactions += 1

    return {
        'pl': pl,
        'totalTransactions': totalTransactions,
        'profitableTransactions': profitableTransactions
    }


def updateTokenStatsWithAdditionalData(tokenStats):
    tokensToHandle = list(
        map(
            lambda token: f"{token}:{networksMap[tokenStats[token]['chain']]}",
            tokenStats.keys(),
        ),
    )
    tokensString = ', '.join(f'"{token}"' for token in tokensToHandle)

    query = '''
    {
        filterTokens(
            limit: 200,
            tokens: [%(tokens)s],
        ) {
            results {
                token {
                    address,
                    explorerData {
                        description,
                    },
                },
                liquidity,
                volume24,
                createdAt,
            }
        }
    }
    ''' % {'tokens': tokensString}

    response = requests.post(
        'https://graph.defined.fi/graphql',
        json={'query': query},
        headers={
            'Authorization': os.getenv('DEFINED_API_KEY')
        }
    )

    results = response.json()['data']['filterTokens']['results']

    additionalTokenStats = {}

    for pair in results:
        token = pair['token']['address']
        additionalTokenStats[token] = pair

    for token in tokenStats:
        if token in additionalTokenStats:
            liquidity = additionalTokenStats[token]['liquidity']
            currentLiquidity = int(liquidity) if liquidity else 0

            dailyVolume = additionalTokenStats[token]['volume24']
            currentDailyVolume = int(dailyVolume) if dailyVolume else 0

            createdAt = additionalTokenStats[token]['createdAt']
            timeDifferenceSeconds = tokenStats[token]['firstBuyUnixTimestamp'] - createdAt

            secondsPassedFromCreationToFirstBuy = timeDifferenceSeconds if timeDifferenceSeconds > 0 else 0

            description = additionalTokenStats[token]['token']['explorerData']['description']

            isScam = currentLiquidity < 10000 or currentDailyVolume < 10000

            tokenStats[token]['liquidityCurrent'] = currentLiquidity
            tokenStats[token]['dailyVolumeCurrent'] = currentDailyVolume
            tokenStats[token]['createdAt'] = createdAt if createdAt else 0
            tokenStats[token]['secondsPassedFromCreationToFirstBuy'] = secondsPassedFromCreationToFirstBuy
            tokenStats[token]['timePassedFromCreationToFirstBuy'] = getSecondsToReadableFormat(secondsPassedFromCreationToFirstBuy)
            tokenStats[token]['description'] = description if description else ''
            tokenStats[token]['isScam'] = isScam
        else:
            tokenStats[token]['liquidityCurrent'] = None
            tokenStats[token]['dailyVolumeCurrent'] = None
            tokenStats[token]['createdAt'] = None
            tokenStats[token]['secondsPassedFromCreationToFirstBuy'] = None
            tokenStats[token]['timePassedFromCreationToFirstBuy'] = None
            tokenStats[token]['description'] = None
            tokenStats[token]['isScam'] = None

def readCSV():
    with open('wallets.csv', 'a+'):
        pass

    with open('wallets.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            wallets[row[0]] = requestData(row[0])

def writeCSV():
    with open('analysis.csv', mode='w', newline='') as analysis_file, open('tokenAnalytics.csv', mode='w', newline='') as token_file:
        # Write PL and winrate by wallet in analysis.csv
        analysis_writer = csv.writer(analysis_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        analysis_writer.writerow([
            'WALLET',
            'LAST TRANSACTION',
            'TRANSACTION COUNT (pairs of buy&sell)',
            'WIN RATE',
            'UNREALIZED WIN RATE',
            'PL',
            'UNREALIZED PL',
            'AVG % GAIN OR LOSS',
            'LARGEST GAIN in $',
            'LARGEST LOSS in $',
            'LOSS STREAK',
            'AVG BUY VOLUME',
            'LIQUIDITY MATCH',
            'AVG LIQUIDITY ON BUY',
            'AVG LIQUIDITY CURRENT',
            'AVG DAILY VOLUME ON BUY',
            'AVG DAILY VOLUME CURRENT',
            'AVG TIME PASSED FROM CREATION TO FIRST BUY',
            '% OF DESCRIPTION',
            'AMOUNT OF TOKENS',
            '% OF SCAM',
        ])

        # Write PL and winrate by wallet by token in tokenAnalytics.csv
        token_writer = csv.writer(token_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        token_writer.writerow([
            'WALLET',
            'TOKEN',
            'ADDRESS',
            'TRANSACTION COUNT',
            'WIN RATE',
            'PL',
            'AVG % GAIN OR LOSS',
            'LIQUIDITY ON BUY',
            'LIQUIDITY CURRENT',
            'DAILY VOLUME ON BUY',
            'DAILY VOLUME CURRENT',
#             'FIRST BUY TIMESTAMP',
#             'CREATED AT',
#             'SECONDS PASSED FROM CREATION TO BUY',
            'DESCRIPTION',
            'TIME PASSED FROM CREATION TO FIRST BUY',
            'IS SCAM',
        ])

        for wallet in wallets:
            if wallets[wallet] is not None:
                analysis_writer.writerow([
                    wallet,
                    wallets[wallet]['dateOfLastTransaction'],
                    wallets[wallet]['totalTransactionCount'],
                    wallets[wallet]['winRate'],
                    wallets[wallet]['unrealizedWinRate'],
                    wallets[wallet]['profit'],
                    wallets[wallet]['unrealizedProfit'],
                    wallets[wallet]['percentageGainOrLoss'],
                    wallets[wallet]['largestGain'],
                    wallets[wallet]['largestLoss'],
                    wallets[wallet]['lossStreak'],
                    wallets[wallet]['avgBuyVolume'],
                    wallets[wallet]['liquidityMatch'],
                    wallets[wallet]['avgLiquidityOnBuy'],
                    wallets[wallet]['avgLiquidityCurrent'],
                    wallets[wallet]['avgDailyVolumeOnBuy'],
                    wallets[wallet]['avgDailyVolumeCurrent'],
                    wallets[wallet]['avgTimePassedFromCreationToFirstBuy'],
                    wallets[wallet]['percentageOfDescription'],
                    wallets[wallet]['tokenAmount'],
                    wallets[wallet]['percentageOfScam'],
                ])

                tokenStats = wallets[wallet]['tokenStats']

                for token in tokenStats:
                    tokenData = tokenStats[token]
                    token_writer.writerow([
                        wallet,
                        tokenData['symbol'],
                        token,
                        tokenData['transactionCount'],
                        tokenData['winRate'],
                        tokenData['pl'],
                        str(tokenData['percentageGainOrLossByToken']) + '%',
                        tokenData['liquidityOnBuy'],
                        tokenData['liquidityCurrent'],
                        tokenData['dailyVolumeOnBuy'],
                        tokenData['dailyVolumeCurrent'],
#                         tokenData['firstBuyUnixTimestamp'],
#                         tokenData['createdAt'],
#                         tokenData['secondsPassedFromCreationToFirstBuy'],
                        tokenData['description'],
                        tokenData['timePassedFromCreationToFirstBuy'],
                        tokenData['isScam'],
                    ])

def main():
    global transactions
    transactions = input('Number of trades to analyze: ')

    readCSV()
    writeCSV()

#     print('WALLETS', wallets)

if __name__ == "__main__":
    main()