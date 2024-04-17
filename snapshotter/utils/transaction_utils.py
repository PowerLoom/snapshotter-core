from snapshotter.settings.config import settings


async def write_transaction(w3, chain_id, address, private_key, contract, function, nonce, *args):
    """ Writes a transaction to the blockchain

    Args:
            w3 (web3.Web3): Web3 object
            address (str): The address of the account
            private_key (str): The private key of the account
            contract (web3.eth.contract): Web3 contract object
            function (str): The function to call
            *args: The arguments to pass to the function

    Returns:
            str: The transaction hash
    """

    # Create the function
    func = getattr(contract.functions, function)
    # Get the transaction
    transaction = await func(*args).build_transaction({
        'from': address,
        'gas': 2000000,
        'gasPrice': w3.to_wei('0.0001', 'gwei'),
        'nonce': nonce,
        'chainId': chain_id,
    })

    # Sign the transaction
    signed_transaction = w3.eth.account.sign_transaction(
        transaction, private_key=private_key,
    )
    # Send the transaction
    tx_hash = await w3.eth.send_raw_transaction(signed_transaction.rawTransaction)
    # Wait for confirmation
    return tx_hash.hex()
