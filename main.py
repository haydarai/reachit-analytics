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
