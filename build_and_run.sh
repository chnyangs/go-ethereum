make geth
# ./build/bin/geth --mainnet --http --http.api eth,net,engine,admin --authrpc.jwtsecret=../jwt.hex

./build/bin/geth --mainnet --http --http.api eth,net,engine,admin --authrpc.addr localhost --authrpc.port 8551 --authrpc.vhosts localhost --authrpc.jwtsecret '/Users/xyan0559/project/ethereum/jwt.hex'