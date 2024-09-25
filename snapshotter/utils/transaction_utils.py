from snapshotter.settings.config import settings


async def write_transaction(w3, chain_id, address, private_key, contract, function, nonce, *args):
    """
    Writes a transaction to the blockchain.

    This function creates, signs, and sends a transaction to the specified blockchain.
    It uses the provided Web3 instance, contract, and function details to build the transaction,
    then signs it with the given private key and sends it to the network.

    Args:
        w3 (web3.Web3): Web3 object for interacting with the blockchain.
        chain_id (int): The ID of the blockchain network.
        address (str): The address of the account initiating the transaction.
        private_key (str): The private key of the account for signing the transaction.
        contract (web3.eth.contract): Web3 contract object to interact with.
        function (str): The name of the function to call on the contract.
        nonce (int): The nonce value for the transaction.
        *args: Variable length argument list for the contract function.

    Returns:
        str: The transaction hash in hexadecimal format.

    Raises:
        AttributeError: If the specified function doesn't exist in the contract.
        ValueError: If there's an issue with transaction parameters.
        web3.exceptions.TransactionNotFound: If the transaction fails to be mined.
    """

    # Create the contract function object
    func = getattr(contract.functions, function)

    # Build the transaction with specified parameters
    transaction = await func(*args).build_transaction({
        'from': address,
        'gas': 2000000,  # Gas limit
        'gasPrice': w3.to_wei('0.0001', 'gwei'),  # Gas price in wei
        'nonce': nonce,
        'chainId': chain_id,
    })

    # Sign the transaction with the account's private key
    signed_transaction = w3.eth.account.sign_transaction(
        transaction, private_key=private_key,
    )

    # Send the raw transaction to the network
    tx_hash = await w3.eth.send_raw_transaction(signed_transaction.rawTransaction)

    # Return the transaction hash in hexadecimal format
    return tx_hash.hex()
