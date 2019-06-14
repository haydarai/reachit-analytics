import os
from mongoengine import connect
from dotenv import load_dotenv
from models.user_model import User
from models.promotion_model import Promotion
from models.transaction_model import Transaction
import os
from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON
import urllib.request
import urllib.parse

load_dotenv()
connect(host=os.getenv('MONGODB_URI'))

neo4j_uri = os.getenv('NEO4J_URI')
neo4j_user = os.getenv('NEO4J_USER')
neo4j_password = os.getenv('NEO4J_PASSWORD')

driver = GraphDatabase.driver(
            neo4j_uri, auth=(neo4j_user, neo4j_password))


def insert_user_and_product(userName, email, productName):
    with driver.session() as session:
        session.run(
            """ Merge (u:User{userName:{userName}, email: {email}}) Merge (p:Product{productName : {productName}})""",
            userName=userName, email=email, productName=productName
        )

def create_index():
    with driver.session() as session:
        session.run("""
        create index on :User(email)
        """)

def insert_transactions (userName, email, productName, type, location, purchaseDate, quantity, price, currency, merchant):
    with driver.session() as session:
        session.run("""
        Merge (p:Product{productName : {productName}})-[:typeOf]->(t:Type{typeName:{type}})
        Merge (u:User{userName:{userName}})-[:bought{location: {location}, purchaseDate: {purchaseDate}, quantity: {quantity}, 
        price: {price}, currency: {currency}, merchant: {merchant}}]->(p)
        """, userName = userName, email = email, productName = productName,
               type = type, location = location, purchaseDate = purchaseDate, quantity = quantity,
               price = price, currency = currency, merchant = merchant)

def get_product_category(product_name):
    sparql.setQuery ( f"""
    PREFIX reachIT: <http://www.reach-it.com/ontology/>
    SELECT str(?c) as ?type
        WHERE {{
                ?product reachIT:productName ?productName.
                ?product reachIT:belongsToCategory ?t.
                ?t reachIT:categoryName ?c .
                FILTER (lang(?c)='en' and lang(?productName)='en' and ?productName={product_name})
        }}
    ORDER BY  DESC(?type)
    LIMIT 1
    """)

    query_results = sparql.query().convert()

    return query_results["results"]["bindings"][0]["type"]["value"]

def build_recommendation():
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

def update_property_graph_transaction():
        promotions = Promotion.objects.all()
        transactions = Transaction.objects.all()
        users = User.objects.all()

        total_transaction = len(transactions)
        print("Total transaction: "+str(total_transaction))
        counter = 1
        for transaction in transactions[93:100]:
                print("Transaction "+str(counter)+" of "+str(total_transaction))
                # print(transaction['user']['name'])
                userName = transaction['user']['name'] #username
                email = transaction['user']['email'] #useremail
                merchant = transaction['merchant'] #merchant
                purchaseDate = str(transaction['created_at']) #purchaseDate
                location = transaction['location'] #location
                for product in transaction['items']:
      
                        product_name = product['name']
                        price = str(product['price'])  #price
                        currency = product['currency']  #currency
                        quantity = str(product['quantity']) #quantity
                        
                        # Retrieve the product type from Knowledge Graph
                        try:
                                url_encoded_product_name = os.getenv("KG_SERVICE_URI")+"productcategory/?product_name="+urllib.parse.quote(product_name) 
                                product_type = urllib.request.urlopen(url_encoded_product_name).read()
                        except expression as identifier:
                                product_type=""
                        # If the product type is not empty, insert it to neo4j
                        if(len(product_type)>0):
                                insert_user_and_product(userName, email, product_name)
                counter = counter + 1
        
        create_index()

        counter = 1
        for transaction in transactions[0:100]:
                print("Transaction "+str(counter)+" of "+str(total_transaction))
                # print(transaction['user']['name'])
                userName = transaction['user']['name'] #username
                email = transaction['user']['email'] #useremail
                merchant = transaction['merchant'] #merchant
                purchaseDate = str(transaction['created_at']) #purchaseDate
                location = transaction['location'] #location
                for product in transaction['items']:
      
                        product_name = product['name']
                        price = str(product['price'])  #price
                        currency = product['currency']  #currency
                        quantity = str(product['quantity']) #quantity
                        
                        # Retrieve the product type from Knowledge Graph
                        try:
                                url_encoded_product_name = os.getenv("KG_SERVICE_URI")+"productcategory/?product_name="+urllib.parse.quote(product_name) 
                                product_type = urllib.request.urlopen(url_encoded_product_name).read()
                        except expression as identifier:
                                product_type=""
                        # If the product type is not empty, insert it to neo4j
                        if(len(product_type)>0):
                                insert_transactions(userName, email, product_name, product_type, location, purchaseDate, quantity, price, currency, merchant)
                counter = counter + 1

if __name__ == "__main__":
        update_property_graph_transaction();
        build_recommendation()
