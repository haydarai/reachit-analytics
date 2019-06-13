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
        Merge (u:User{userName:{userName}, email: {email}}) Merge (p:Product{productName : {productName}, type: {type}})  
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

