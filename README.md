# gnosis-safe-stats
Pulling some stats about deployed Safes from the blockchain

## 0: Prerequisites

- Install dependecies: `pip install -r requirements.txt` 
- Add Etherscan and Infure API keys to `gnosis_safe_stats.py`

## 1 (Optional): Check for new tokens on the Safe relay service
- Execute script: `python update_tokens.py`

## 2 (Optional): Check for new Safes on etherscan
- Execute script: `python update_safes.py`

## 3: Fetch stats
- Execute script: `python gnosis_safe_stats.py > stats.csv`
