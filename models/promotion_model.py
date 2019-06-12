import mongoengine as db
import mongoengine_goodjson as gj
from datetime import datetime


class Promotion(gj.Document):
    creator = db.ReferenceField('User')
    title = db.StringField(required=True)
    description = db.StringField()
    product_type = db.StringField()
    image = db.StringField()
    city = db.StringField()
    country = db.StringField()
    start_valid_date = db.DateTimeField(default=datetime.utcnow)
    end_valid_date = db.DateTimeField()
    created_at = db.DateTimeField(default=datetime.utcnow)
    last_modified_at = db.DateTimeField(default=datetime.utcnow)
    meta = {'collection': 'promotions',
            'indexes': [{'fields': ['$product_type']}]}
