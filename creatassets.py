#!/usr/bin/env python3

import json
import random
import string
import uuid
import sys


def rand_name(length=10):
    #return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    #Python 2.7
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

def gen_asset_file(cust_id, line_num=50):
    domain = 'kittennet'
    asset_types = ['user', 'host']
    for at in asset_types:
        if at == 'host':
            domain = 'mandiant'
        with open('{}_{}assets.json'.format(cust_id, at), 'w') as out_file:
            for _ in range(line_num):
                name = rand_name()
                line = {}
                line['customer_id'] = cust_id
                line['asset_type'] = at
                line['name'] = name
                line['aliases'] = [name]
                line['tags'] = ['critical']
                line['properties'] = {"domain": domain}
                line['asset_uuid'] = str(uuid.uuid4())
                json.dump(line, out_file)
                out_file.write('\n')

cust_id = sys.argv[1]
gen_asset_file(cust_id)
# To run the script make sure the file is executable then run like this:
# ./create_assets.py 'hexint07sust01'
# you can pass whichever customer id you would like. this will generate a host assets and user assets with line_num assets

