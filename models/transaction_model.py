import mongoengine as db
import mongoengine_goodjson as gj
from datetime import datetime


class Item(db.EmbeddedDocument):
    name = db.StringField(required=True)
    currency = db.StringField(required=True)
    price = db.DecimalField(required=True)
    quantity = db.DecimalField()


class Transaction(gj.Document):
    user = db.ReferenceField('User')
    items = db.ListField(db.EmbeddedDocumentField(Item))
    location = db.StringField()
    merchant = db.StringField()
    city = db.StringField()
    country = db.StringField()
    created_at = db.DateTimeField(default=datetime.utcnow)
    last_modified_at = db.DateTimeField(default=datetime.utcnow)
    meta = {'collection': 'transactions',
            'shard_key': ('country',), 'indexes': [{'fields': ['$user']}]}
