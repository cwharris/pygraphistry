cat ./compose/neo4j/edges.csv \
    | cut -d, -f1-5 \
    | sed -E "s/([a-z0-9]{32})\.\.\./\1/g" \
    | sed -E "s/([a-z0-9]{32})([a-z0-9]{8})/\1/g" \
    | sed "s/2a37b3bdca935152335c2097e5da367d/Ross Ulbricht (SilkRoad)/g" \
    | sed "s/b2233dd22ade4c9978ec1fd1fbb36eb7/Carl Force (DEA)/g" \
    | sed "s/Amount \\$/USD/g" \
    | sed -E "s/(Tainted Coins|USD[a-zA-Z_]*?)(,)?/\1:FLOAT\2/g" \
    > ./compose/neo4j/edges-clean.csv

cat ./compose/neo4j/nodes.csv \
    | cut -d, -f1-5 \
    | sed -E "s/([a-z0-9]{32})\.\.\./\1/g" \
    | sed -E "s/([a-z0-9]{32})([a-z0-9]{8})/\1/g" \
    | sed "s/2a37b3bdca935152335c2097e5da367d/Ross Ulbricht (SilkRoad)/g" \
    | sed "s/b2233dd22ade4c9978ec1fd1fbb36eb7/Carl Force (DEA)/g" \
    | sed "s/Balance \\$/USD/g" \
    | sed "s/Balance (avg) \\$/USD_avg/g" \
    | sed "s/Balance (max) \\$/USD_max/g" \
    | sed -E "s/(Tainted Coins|USD[a-zA-Z_]*?)(,)?/\1:FLOAT\2/g" \
    > ./compose/neo4j/nodes-clean.csv


#    | sed "s/Date/Date:DATETIME/g" \