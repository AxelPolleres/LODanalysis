from collections import defaultdict
import requests
import sys
import json
import csv
import simplejson
from SPARQLWrapper import SPARQLWrapper, JSON


# uncomment to load datasets from a local file
with open('lod_datahub.io_enrichted.json', 'r', encoding='UTF-8') as fp:
    datasets=json.load(fp)

voidcnt = 0
dscnt = 0

for ds in datasets:
    for i in ds['results']:
        hasvoid=False
        for x in i['resources']:
            address = x['url'].strip()
            if address.startswith('http') :
                slashes_address = address.split('/')
                void_address=slashes_address[0] + '/' + slashes_address[1] + '/' + slashes_address[2] + "/.well-known/void"
                if ' ' in slashes_address[2] or not '.' in slashes_address[2]:
                    print("ERROR: invalid server/domain: "+slashes_address[2])
                else:
                    x['void_address']= void_address
                    # print("curl -L  -H 'Accept: text/turtle, application/n-triples, application/trig, application/n-quads, application/rdf+xml, *' "+void_address+" -o "+slashes_address[2]+'_void')
                    resp = requests.get(void_address, allow_redirects=True, stream=True, timeout=20)
                    print(resp)
                    if (resp.status_code >= 200 and resp.status_code < 300):
                        x['voidresponse'] = resp
                        print(resp)
                        hasvoid = True
                    else:
                        x['voiderr']=("Response-status: "+str(resp.status_code)+":\n "+str(resp.headers))
        if hasvoid:
            voidcnt = voidcnt+1
        dscnt = dscnt+1

