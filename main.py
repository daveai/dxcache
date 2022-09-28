from web3 import Web3
from dotenv import load_dotenv
import pandas as pd
import os, json, requests

# Load environment variables
load_dotenv()


def fetch_cache(endpoint, contributor_scheme, vm, fromBlock):
    """Fetches DXdao contributor proposal data

    Args:
        endpoint (string): Websocket endpoint
        contributor_scheme (string): address of the contributor scheme
        vm (string): address of the voting machine
        fromBlock (int): starting block

    Returns:
        df: DataFrame with all successful contributor proposals
    """
    # Connect to the HTTP node
    w3 = Web3(Web3.WebsocketProvider(endpoint))

    # Create smart contract interfaces
    cs = w3.eth.contract(address=contributor_scheme, abi=cs_abi)
    vm = w3.eth.contract(address=vm, abi=vm_abi)

    # Get all proposals events from the Contribution Scheme
    events = cs.events.NewContributionProposal.getLogs(fromBlock=fromBlock)
    data = [i["args"] for i in events]

    # Turn json into pandas DataFrame
    df = pd.DataFrame(data)

    # Turn _proposalId bytes into a hex string and prefix with 0x
    df["_proposalId"] = df["_proposalId"].apply(lambda x: "0x" + x.hex())

    # Filter out proposals that were not successful
    # Get the proposal state from the voting machine (2 = executed)
    df["proposalState"] = df["_proposalId"].apply(
        lambda x: vm.functions.state(x).call()
    )
    # Get the proposal outcome from the voting machine (1 = passed)
    df["winningVote"] = df["_proposalId"].apply(
        lambda x: vm.functions.winningVote(x).call()
    )
    # Drop rows where proposalState != 2 or winningVote != 1
    df = df[(df["proposalState"] == 2) & (df["winningVote"] == 1)]

    # Split _rewards into 5 columns
    df[
        [
            "useless_dxd_reward",
            "eth_reward",
            "erc20_reward",
            "period_length",
            "period_num",
        ]
    ] = pd.DataFrame(df["_rewards"].tolist(), index=df.index)

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
            "title",
            "description",
            "_beneficiary",
            "_reputationChange",
            "eth_reward",
            "erc20_reward",
            "_externalToken",
        ]
    ]

    return df


# If file is executed directly
if __name__ == "__main__":
    ipfs_gateway = os.getenv("IPFS_GATEWAY")
    cs_abi = json.load(open("abis/contribution_scheme.json"))
    vm_abi = json.load(open("abis/vm_abi.json"))
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
        os.getenv("RPC_MAIN"),
        "0x08cC7BBa91b849156e9c44DEd51896B38400f55B",
        "0x332B8C9734b4097dE50f302F7D9F273FFdB45B84",
        int(fromBlock),
    )
    fromBlock = requests.get(
        "https://api.gnosisscan.io//api?module=block&action=getblocknobytime&timestamp="
        + str(start)
        + "&closest=before&apikey=YourApiKeyToken"
    ).json()["result"]
    gnosis = fetch_cache(
        os.getenv("RPC_GNOSIS"),
        "0x016Bf002D361bf5563c76230D19B4DaB4d66Bda4",
        "0xDA309aDF1c84242Bb446F7CDBa96B570E901D4CF",
        int(fromBlock),
    )

    # Merge mainnet and gnosis dataframes
    df = pd.concat([mainnet, gnosis], ignore_index=True)
    # Save to csv
    df.to_csv("data.csv", index=False)
