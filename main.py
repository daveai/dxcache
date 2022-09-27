from web3 import Web3
from dotenv import load_dotenv
import pandas as pd
import os
import json
import requests

# Load environment variables
load_dotenv()


def fetch_cache(endpoint, contributor_scheme, fromBlock):
    # Connect to the HTTP node
    w3 = Web3(Web3.WebsocketProvider(endpoint))

    # Create smart contract interface
    cs = w3.eth.contract(address=contributor_scheme, abi=cs_abi)

    # Get all proposals events from the Contribution Scheme
    events = cs.events.NewContributionProposal.getLogs(fromBlock=int(fromBlock))
    data = [i["args"] for i in events]

    # Turn json into pandas DataFrame
    df = pd.DataFrame(data)

    # Split _rewards into 5 columns
    df[
        [
            "useless_dxd_reward",
            "eth_reward",
            "erc20_reward",
            "period_length",
            "period_numb",
        ]
    ] = pd.DataFrame(df["_rewards"].tolist(), index=df.index)
    # Turn _proposalId bytes into a hex string and prefix with 0x
    df["_proposalId"] = df["_proposalId"].apply(lambda x: "0x" + x.hex())
    # Drop all columns with 0 values
    df = df.loc[:, (df != 0).any(axis=0)]

    # Retreive proposal data from _descriptionHash IPFS hash, if error set to 'IPFS error'
    df["proposal_data"] = df["_descriptionHash"].apply(
        lambda x: requests.get(ipfs_gateway + x).json()
        if requests.get(ipfs_gateway + x).status_code == 200
        else "IPFS error"
    )
    # Split proposal data into 4 columns - passes list of keys to make sure they are in the correct order
    df[
        pd.DataFrame(df["proposal_data"].tolist(), index=df.index).columns
    ] = pd.DataFrame(df["proposal_data"].tolist(), index=df.index)
    # Keep only required cols
    df = df[
        [
            "_avatar",
            "_proposalId",
            "_title",
            "_description",
            "_beneficiary",
            "_reputationChange",
            "eth_reward",
            "erc20_reward",
            "erc20_address",
        ]
    ]

    return df


# If file is executed directly
if __name__ == "__main__":
    ipfs_gateway = os.getenv("IPFS_GATEWAY")
    cs_abi = json.load(open("abis/contribution_scheme.json"))
    # Ask user to input start_date in DD/MM/YYYY format
    start_date = input("Enter start date in MM/DD/YYYY format: ")
    # Convert start_date to unix timestamp
    start = int(pd.to_datetime(start_date).timestamp())
    # turn unix timestamp into block number
    fromBlock = requests.get(
        "https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp="
        + str(start)
        + "&closest=before&apikey=YourApiKeyToken"
    ).json()["result"]
    mainnet = fetch_cache(
        os.getenv("RPC_MAIN"), "0x08cC7BBa91b849156e9c44DEd51896B38400f55B", fromBlock
    )
    fromBlock = requests.get(
        "https://api.gnosisscan.io//api?module=block&action=getblocknobytime&timestamp="
        + str(start)
        + "&closest=before&apikey=YourApiKeyToken"
    ).json()["result"]
    gnosis = fetch_cache(
        os.getenv("RPC_GNOSIS"), "0x016Bf002D361bf5563c76230D19B4DaB4d66Bda4", fromBlock
    )

    # Merge mainnet and gnosis dataframes
    df = pd.concat([mainnet, gnosis], ignore_index=True)
    # Save to csv
    df.to_csv("data.csv", index=False)
