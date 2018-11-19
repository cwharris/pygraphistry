FROM neo4j:3.5 AS build

RUN echo 'dbms.directories.data=data-persistent' > conf/neo4j.conf

COPY ./compose/neo4j/nodes-clean.csv nodes.csv
COPY ./compose/neo4j/edges-clean.csv edges.csv

RUN cat edges.csv \
    | sed -E "s/([a-z0-9]{32})\.\.\./\1/g" \
    | sed -E "s/([a-z0-9]{32})([a-z0-9]{8})/\1/g" \
    | sed "s/2a37b3bdca935152335c2097e5da367d/Ross Ulbricht (SilkRoad)/g" \
    | sed "s/b2233dd22ade4c9978ec1fd1fbb36eb7/Carl Force (DEA)/g" \
    | sed "s/Amount \\$/USD/g" \
    | cut -d, -f1-5 \
    > edges-clean.csv

RUN cat nodes.csv \
    | sed -E "s/([a-z0-9]{32})\.\.\./\1/g" \
    | sed -E "s/([a-z0-9]{32})([a-z0-9]{8})/\1/g" \
    | sed "s/2a37b3bdca935152335c2097e5da367d/Ross Ulbricht (SilkRoad)/g" \
    | sed "s/b2233dd22ade4c9978ec1fd1fbb36eb7/Carl Force (DEA)/g" \
    | sed "s/Balance \\$/USD/g" \
    | sed "s/Balance (avg) \\$/USD_avg/g" \
    | sed "s/Balance (max) \\$/USD_max/g" \
    | cut -d, -f1-5 \
    > nodes-clean.csv

RUN neo4j-admin import \
    --mode csv \
    --nodes:ACCOUNT nodes.csv \
    --relationships:PAYMENT edges.csv

RUN rm nodes.csv nodes-clean.csv
RUN rm edges.csv edges-clean.csv

RUN chown -R neo4j /var/lib/neo4j/data-persistent
