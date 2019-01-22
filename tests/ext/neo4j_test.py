import graphistry

def test_neo4j():
    graphistry.register(
        protocol = 'http',
        server = 'localhost',
        bolt = { 'uri': 'bolt://localhost:7687' }
    )

    graphistry \
        .cypher("MATCH (a)-[r:PAYMENT]->(b) RETURN distinct * LIMIT 1") \
        .plot()
