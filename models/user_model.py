import mongoengine as db
import mongoengine_goodjson as gj


class User(gj.Document):
    email = db.StringField(required=True, primary_key=True)
    name = db.StringField(required=True, max_length=100)
    user_type = db.StringField(required=True, choices=('user', 'merchant'))
    password = db.StringField(required=True)
    meta = {'collection': 'users'}
