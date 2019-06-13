import os
from mongoengine import connect
from dotenv import load_dotenv
from models.user_model import User
from models.promotion_model import Promotion
from models.transaction_model import Transaction

load_dotenv()

connect(host=os.getenv('MONGODB_URI'))

promotions = Promotion.objects.all()
transactions = Transaction.objects.all()
users = User.objects.all()

# TODO: import all MongoDB documents to Neo4J


import os
from neo4j import GraphDatabase


uri = "bolt://localhost:7687"
user = "neo4j"
password = "pinar123"

driver = GraphDatabase.driver(
            uri, auth=(user, password))


def insertNeo4j (userName, email, productName, type, location, purchaseDate, quantity, price, currency, merchant):
    with driver.session() as session:
        session.run("""
        Merge (u:User{userName:{userName}, email: {email}}) Merge (p:Product{productName : {productName}})  
        Merge (p)-[:typeOf]-(t:Type{typeName:{type}})
        Merge (u)-[:bought {location: {location}, purchaseDate: {purchaseDate}, quantity: {quantity}, 
        price: {price}, currency: {currency}, merchant: {merchant}}]->(p)
        """, userName = userName, email = email, productName = productName,
               type = type, location = location, purchaseDate = purchaseDate, quantity = quantity,
               price = price, currency = currency, merchant = merchant)

from SPARQLWrapper import SPARQLWrapper, JSON
sparql = SPARQLWrapper("http://10.6.0.119:8890/sparql")
sparql.setReturnFormat(JSON)
sparql.addDefaultGraph('http://localhost:8890/reach-it')



def kg_retrieve(value):
    sparql.setQuery ( f"""
    PREFIX reachIT: <http://www.reach-it.com/ontology/>
    SELECT str(?c) as ?type
    WHERE {{
    ?product reachIT:productName ?productName.
    ?product reachIT:belongsToCategory ?t.
    ?t reachIT:categoryName ?c .
    FILTER (lang(?c)='en' and lang(?productName)='en' and ?productName={value})
    }}
    order by  desc(?type)
    limit 1
    """)

    query_results = sparql.query().convert()

    return query_results["results"]["bindings"][0]["type"]["value"]


for transaction in transactions:
    print("whatever")
    # print(transaction['user']['name'])
    userName = transaction['user']['name'] #username
    email = transaction['user']['email'] #useremail
    merchant = transaction['merchant'] #merchant
    purchaseDate = str(transaction['created_at']) #purchaseDate
    location = transaction['location'] #location
    for product in transactions[0]['items']:
        productName = product['name']  #productName
        type = kg_retrieve(f"'{productName}'@en")
        price = str(product['price'])  #price
        currency = product['currency']  #currency
        quantity = str(product['quantity']) #quantity
        insertNeo4j(userName, email, productName, type, location, purchaseDate, quantity, price, currency, merchant)


call_algorithms()

def call_algorithms ():
    with driver.session() as session:
        session.run("""        
            MATCH (u:User)-[ur:bought]->(p:Product)--(t:Type)
            WITH {item:id(u), categories: collect(id(t))} as userData
            WITH collect(userData) as data
            CALL algo.similarity.jaccard.stream(data, {topK: 3, similarityCutoff: 0.7})
            YIELD item1, item2, count1, count2, intersection, similarity
            With algo.asNode(item1) AS from, algo.asNode(item2) AS to, similarity
            merge (from)-[r:similarTo]-> (to)
        """)
        session.run("""        
                CALL algo.louvain.stream('User', 'similar', {})
                YIELD nodeId, community
                with algo.asNode(nodeId) AS user, community, nodeId
                SET user.community = community
         """)
        session.run("""  
                match (u:User)-[r:bought]-(p:Product)--(t:Type) 
                with   u.community as com, t.typeName as type, count(id(p)) as prod
                order by com, prod desc 
                with com, collect(type) as type_collect 
                with com, type_collect[..3] as max_type
                unwind max_type as new_type
                match (us:User{community:com}) 
                merge (us)-[:getPromotion]-(t:Type{typeName:new_type})
         """)
