from web3 import Web3
import pandas as pd
import json
import requests

def fetch_cache(endpoint, contributor_scheme, fromBlock):
    # Connect to the HTTP node
    w3 = Web3(Web3.WebsocketProvider(endpoint))

    # Create smart contract interface 
    cs = w3.eth.contract(address=contributor_scheme, abi=cs_abi)

    # Get all proposals events from the Contribution Scheme
    events = cs.events.NewContributionProposal.getLogs(fromBlock=int(fromBlock))
    data = [i['args'] for i in  events]

    # Turn json into pandas DataFrame
    df = pd.DataFrame(data)

    # Split _rewards into 5 columns
    df[['useless_dxd_reward', 'eth_reward', 'erc20_reward', 'period_length', 'period_numb']] = pd.DataFrame(df['_rewards'].tolist(), index= df.index)
    # Turn _proposalId bytes into a hex string and prefix with 0x
    df['_proposalId'] = df['_proposalId'].apply(lambda x: '0x' + x.hex())
    # Drop all columns with 0 values
    df = df.loc[:, (df != 0).any(axis=0)]

    # Retreive proposal data from _descriptionHash IPFS hash, if error set to 'IPFS error'
    df['proposal_data'] = df['_descriptionHash'].apply(lambda x: requests.get(ipfs_gateway + x).json() if requests.get(ipfs_gateway + x).status_code == 200 else 'IPFS error')
    # Split proposal data into 4 columns
    df[pd.DataFrame(df['proposal_data'].tolist(), index= df.index).columns] = pd.DataFrame(df['proposal_data'].tolist(), index= df.index)
    # Drop proposal_data, url and tags, _intVoteInterface, _rewards
    df = df.drop(['proposal_data', 'url', 'tags', '_intVoteInterface', '_rewards'], axis=1)
    return df

# If file is run as main 
if __name__ == '__main__':
    ipfs_gateway = 'https://davi.mypinata.cloud/ipfs/'
    cs_abi = json.load(open('abis/contribution_scheme.json'))
    # Ask user to input start_date in DD/MM/YYYY format
    start_date = input('Enter start date in MM/DD/YYYY format: ')
    # Convert start_date to unix timestamp
    start = int(pd.to_datetime(start_date).timestamp())
    # turn unix timestamp into block number
    fromBlock = requests.get('https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp=' + str(start) + '&closest=before&apikey=YourApiKeyToken').json()['result']
    mainnet = fetch_cache('wss://eth-mainnet.g.alchemy.com/v2/0nY-8QtxlISlugKf9W9NVF8V7oyrIgfC', '0x08cC7BBa91b849156e9c44DEd51896B38400f55B', fromBlock)
    fromBlock = requests.get('https://api.gnosisscan.io//api?module=block&action=getblocknobytime&timestamp=' + str(start) + '&closest=before&apikey=YourApiKeyToken').json()['result']
    gnosis = fetch_cache('wss://rpc.gnosischain.com/wss', '0x016Bf002D361bf5563c76230D19B4DaB4d66Bda4', fromBlock)

    # Merge mainnet and gnosis dataframes
    df = pd.concat([mainnet, gnosis], ignore_index=True)
    # Save to csv
    df.to_csv('data_{start_date}.csv', index=False)