#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  app.py
#
#  Copyleft 2017 Mali Media Group
#  <http://malimedia.be>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program. If not, see <http://www.gnu.org/licenses/>.
#
#############################################################################
#
#  CampaignEntries - campaign_entries
#  
#  Chalice app to allow for adding of emailadressess/contacts via campaigns.
#  Campaigns can be both internal of external.
#
#  It checks for data-validity, email existence in the db and responds.
#  It saves the provided data as JSON to S3.
#
#############################################################################
#
#   TODO:
#   - improve HTTP status codes in responses
#   - streamline creation of response format
#
#############################################################################

from datetime import datetime
import logging
import json

from chalice import Chalice
from chalice.app import ChaliceError
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from chalicelib import DEA_DOMAIN_LIST

app = Chalice(app_name='campaign_entries')
app.debug = True
log = app.log
log.setLevel(logging.DEBUG)

# Create some custom Exceptions
class CustomChaliceError(ChaliceError):
    STATUS_CODE = 201
    def __init__(self, msg=''):
        super(CustomChaliceError, self).__init__(
            self.__class__.__name__ + ': %s' % msg)

class DuplicateEntryError(CustomChaliceError):
    STATUS_CODE = 409

# Initialize the response as a global variable
class Response(object):
    pass

# Initialize the DDB-client per container
ddb_client = boto3.client('dynamodb', region_name='eu-central-1', 
                endpoint_url="https://dynamodb.eu-central-1.amazonaws.com")

dynamodb_table_name = 'Emails'

## ---------------------- FUNCTIONS ---------------------- ##

def get_required_fields(campaign):
    return set(campaign['RequiredFields']['SS'])
    
def has_required_fields(data, campaign):
    required_fields = get_required_fields(campaign)
    missing = required_fields.difference(set(data.keys()))
    log.debug("Missing fields: %s", missing)
    if not missing:
        return {'success': True}
    else:
        data = {"reason": "Missing field(s)"}
        data.update({f: "This field is required" for f in missing})
        return {
            'success': False,
            'response': {
                "status" : "fail",
                "data" : data,
            }
        }

def get_contact_from_dynamodb(email=None):
    response = ddb_client.get_item(
        TableName=dynamodb_table_name,
        Key={'Email': {'S': email}}
    )
    try:
        item = response['Item']
    except KeyError as e:
        log.info('Email not found: "%s".', email)
        return {'success': False}
    else:
        log.info('Email found: "%s".', item)
        return {'success': True,
                'response': item['Email']['S']}

def add_to_dynamodb(email=None, time_stamp=None):
    response = ddb_client.put_item(
        TableName=dynamodb_table_name,
        Item={
            'Email': {'S': email},
            'TimeStamp': {'S': time_stamp}
        }
    )
    log.info('Item added with email "%s", RequestId was: %s.',
             email,
             response['ResponseMetadata']['RequestId'])

def lookup_email_in_simpledb(email):
    sdb_client = boto3.client('sdb')
    response = sdb_client.get_attributes(
        DomainName = dynamodb_table_name,
        ItemName   = email,
    )
    try:
        item = response['Attributes']
    except KeyError as e:
        return {'success': False}
    else:
        return {'success': True,
                'response': item}

def add_contact_to_S3(json_body):
    bucket = 'bdm-events'
    prefix = 'leads/%s/' % json_body['campaign']['CampaignShortName']
    log.debug('Prefix is: %s', prefix)
    s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))
    # Key = email + timestamp 2016-10-31T14:33:58.152256Z (ms precision)
    key = prefix + '_'.join([
        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        json_body['data']['email'],
        ]) + '.json'
    log.debug('Key is %s', key)
    func_params = {
        'ACL'   : 'private',
        'Bucket': 'bdm-events',
        #~ 'ServerSideEncryption' : 'AES256'|'aws:kms', TODO: implement data encryption at rest!
        'Key'   : key,
        'ContentType' : 'application/json',
        'ContentEncoding' : 'utf-8',
        'Body' : json.dumps(json_body, indent=4, separators=(',', ': ')),
    }
    try:
        s3_client.put_object(**func_params)
    except Exception as e:
        log.warn(e)
        return False
    else:
        return True

def validate_structure(data, campaign):
    result = has_required_fields(data, campaign)
    if not result['success']:
        return result
    else:
        return {'success': True}

def validate_data(data):
    if data['email'].split('@')[1].lower() in DEA_DOMAIN_LIST:
        return {
            'success': False,
            'response': {
                "status" : "fail",
                "data" : {
                    "lead": "rejected",
                    "reason": "Disposable email address detected: {d}".format(d=data['email'].split('@')[1]),
                    "email": data['email']
                }
            }
        }
    return {'success': True}

def canonicalize(data):
    data['email'] = data['email'].lower().strip()
    return {'success': True}

def lookup_email(data):
    result = get_contact_from_dynamodb(email=data['email'].lower().strip())
    #~ result = lookup_email_in_simpledb(email=data['email'].lower().strip())
    return result

def post_to_S3(data):
    add_contact_to_S3(data)

def handle_data(data, campaign):
    time_stamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    # Validate structure
    result = validate_structure(data, campaign)
    if not result['success']:
        return result['response']
    # Validate data
    result = validate_data(data)
    if not result['success']:
        return result['response']
    # Canonicalize
    result = canonicalize(data)
    if not result['success']:
        return result['response']
    # Lookup email
    result = lookup_email(data)
    if result['success']:
        log.info('Duplicate email: %s.', data['email'])
        lead_data = {
            "status" : "fail",
            "data" : {
                "lead": "rejected",
                "reason": "Duplicate contact",
                "email": result['response']
            }
        }
        #~ raise DuplicateEntryError('Duplicate entry: email "%s" already exists.' % data['email'])
    else:
        log.info('All good: %s will be added.', data['email'])
        add_to_dynamodb(email=data['email'].lower().strip(), time_stamp=time_stamp)
        lead_data = {
            "status": "success",
            "data" : {
                "lead": "accepted",
                "email":  data['email'],
            }
        }
    # Post to S3
    post_data = {
        'data': data,
        'lead': lead_data,
        'campaign': {
            'CampaignShortName': campaign['CampaignShortName']['S'],
            'CampaignDecimal'  : campaign['CampaignDecimal']['N'],
        },
        'meta': {
            'time_stamp': time_stamp,
            'context': app.current_request.context,
            'headers': dict(app.current_request.headers)
        }
    }
    post_to_S3(post_data)
    return lead_data

## ---------------------- ROUTES    ---------------------- ##

@app.route('/')
def index():
    return {'hello': 'world'}

@app.route('/contacts', methods=['POST'], api_key_required=True)
def post_contacts():
    """ Main function. Should always get (via POST) a JSON-body as such:
        ```json
        {"data":
            {"firstname": "John",
             "lastname": "Doe",
             "email": "john@example.com",
             "dob": "1990-10-20",
             "source_ip": "123.45.67.89",
            },
        }
        ```
        Ie., one (and only one, for now) Contact
    """
    # Initialize response object
    #~ response = Response()
    # Get the requests payload as JSON
    jb    = app.current_request.json_body
    log.debug('JSON: %s', jb)
    log.debug('Headers: %s', app.current_request.headers)
    campaign_token = app.current_request.headers['x-api-key']
    try:
        data = jb['data']
    except KeyError as e:
        m = 'Invalid JSON structure: no root element "data" provided.'
        log.warn(m)
        return {
            'success': False,
            'response': {
                "status" : "fail",
                "data" : {"data": m}
            }
        }
    # Get Campaign parameters for this token
    response = ddb_client.get_item(
        TableName='EntryCampaigns',
        Key={'CampaignToken': {'S': campaign_token}}
    )
    try:
        campaign = response['Item']
    except KeyError as e:
        # This exception should already have been "caught" by the API Gateway
        log.warn('CampaignToken not found: "%s".', campaign_token)
        raise Exception('CampaignToken not found: "%s".' % campaign_token)
    else:
        log.debug('Campaign is: %s', campaign['CampaignShortName']['S'])
    log.info('Handling record: %s', data)
    response = handle_data(data, campaign)
    return response


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
