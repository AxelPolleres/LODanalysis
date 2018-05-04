from collections import defaultdict
import requests
import requests_ftp
import sys
import json
import simplejson

# uncomment to store datasets computed in the cell above to a local file
#from ckanapi import RemoteCKAN
#import pprint
#
#datahub = RemoteCKAN('https://old.datahub.io')
#i=0
#r=100
#cnt=100
#datasets = []
#while cnt==100:
#    ds = datahub.call_action('package_search', {'fq': 'tags:lod','start': i,'rows': r })
#    datasets.append(ds)
#    cnt=len(ds['results'])
#    i += r
#
# with open('lod_datahub.io.json', 'w', encoding='UTF-8') as fp:
#  json.dump(datasets, fp)

# uncomment to load datasets from a local file
with open('lod_datahub.io.json', 'r', encoding='UTF-8') as fp:
    datasets=json.load(fp)

# find all resource urls of datasets that have 'rdf dumps of different forms' 
# try different formats
# try conneg, if you get back html
# use head requests first to not download all data at once.
# flag the resources where no RDF is available.

rdfsuffixes=['.ttl','.rdf','.trig','.nt','.nq']
compressedsuffixes=['.bz2','.gz','.bz','.zip']
othersuffixes=['.csv','.json']

dumpurls = defaultdict(list)
formats = defaultdict(list)
suffixes = defaultdict(list)
cnt=0
address=""
for ds in datasets:
    for i in ds['results']:
        for x in i['resources']:
            cnt = cnt+1
            address = x['url'].strip()
            if address in dumpurls:
                break
            else:
                dumpurls[address]=x
            print(str(cnt)+": "+address)
            sys.stdout.flush()
            f=x['format'].lower()
            if ("ttl" in f) or ("turtle" in f) or ("ntriples" in f) or ("n-triples" in f):
                x['guessedformat']='ttl'
            elif ("trig" in f):
                x['guessedformat']='trig'
            elif ("json" in f) or ("json-ld" in f):
                x['guessedformat']='jsonld'
            elif ("nquads" in f) or ("n-quads" in f):
                x['guessedformat']='nq'
            elif ("rdf" in f) or ("owl" in f):
                x['guessedformat']='rdfxml'
            elif ("sparql" in f):
                x['guessedformat']='sparql'
            elif ("html" in f):
                x['guessedformat']='html'
            elif ("xml" in f):
                x['guessedformat']='xml'
            else:
                x['guessedformat']='otherformat'
            formats[x['guessedformat']].append(f)
            
            if ('gz' in f) or ('gzip' in f):
                x['guessedcompression']='gz'
            elif ('bz' in f) or ('bz2' in f) or ('bzip' in f):
                x['guessedcompression']='bz'
            elif ('zip' in f):
                x['guessedcompression']='zip'
            elif ("hdt" in f):
                x['guessedcompression']='hdt'
            else:
                x['guessedcompression']='none'
            
            s=address.lower()
            if s.endswith(('tar.gz','tgz')):
                x['guessedsuffix']='tgz'                
            elif s.endswith(('gz','gzip')):
                x['guessedsuffix']='gz'
            elif s.endswith(('bz2','bz','bzip')):
                x['guessedsuffix']='bz'
            elif s.endswith('zip'):
                x['guessedsuffix']='zip'
            elif s.endswith('hdt'):
                x['guessedsuffix']='hdt'
            elif s.endswith(('htm','html')):
                x['guessedsuffix']='html'
            elif s.endswith(('xml')):
                x['guessedsuffix']='xml'
            elif s.endswith(('ttl','turtle','nt','ntriples')):
                x['guessedsuffix']='ttl'
            elif s.endswith(('nq','nquad','nquads')):
                x['guessedsuffix']='nq'
            elif s.endswith(('trig')):
                x['guessedsuffix']='trig'
            elif s.endswith(('rdf','rdfxml','.owl')):
                x['guessedsuffix']='rdfxml'
            elif s.endswith(('json','jsonld','json-ld')):
                x['guessedsuffix']='jsonld'
            elif s.endswith(('pdf','csv','tsv')):
                x['guessedsuffix']='x_pdf_csv_tsv'
            elif len(s.split('.')[-1])<10:
                x['guessedsuffix']='othersuffix'
            else:
                x['guessedsuffix']='nosuffix'

            if x['guessedsuffix'] in ['tgz','gz','bz','zip']:
                if len(s.split('.')[-2]) < 6:
                    if(s.split('.')[-2] in ['nt','ttl','rdf','owl','hdt','nq','trig']):
                        x['guessedsuffix']=s.split('.')[-2]+"."+x['guessedsuffix']
                    else:
                        x['guessedsuffix']="x_"+x['guessedsuffix']
                        # do not consider unparseable zipped URLs?
                        # del dumpurls[address]

            suffixes[x['guessedsuffix']].append(s)

            #Does the address contain the string "sparql"? 
            if 'sparql' in s:
                x['guessedsparql'] = True
            else:
                x['guessedsparql'] = False
                
            #Does the address contain the string "dump"? 
            if 'dump' in s:
                x['guesseddump'] = True
            else:
                x['guesseddump'] = False
            
            # Check where the guessed format matches the guessed suffix:                
            if(x['guessedsuffix'].startswith('x_')):
                x['readableformat']='nonreadable'
            elif (x['guessedsuffix']==x['guessedformat']):
                x['readableformat']=x['guessedformat']
            else:
                x['readableformat']='check'
                
            if x['readableformat'] != 'nonreadable':
                try:
                    if address.startswith('http'):
                        resp = requests.head(address, allow_redirects=True, stream=True, timeout=10, headers = {"Range": "bytes=0-10000"})
                    elif  address.startswith('ftp'):
                        address_dir=address[0:address.rfind('/')+1]
                        resp = s.list(address)
                    if (resp.status_code/100 >= 200 and resp.status_code/100 < 300):
                            x['headresponse']=resp.status_code
                            print("OK")
                        else:
                            print("Response-status: "+str(resp.status_code)+":\n "+str(resp.headers))
                            x['headerr']=resp.json()
		    else:
			x['headerr']="unknown protocol (non http(s) nor ftp)"
                except simplejson.errors.JSONDecodeError:
                    x['headerr']="JSON decode errorfor response: "+address+" : "+str(resp.raw.read(100))
                except requests.exceptions.HTTPError as errh:
                    x['headerr']=address+" raised error: Http Error:"+str(errh)
                except requests.exceptions.ConnectionError as errc:
                    x['headerr']=address+" raised error: Http Error:"+str(errc)
                except requests.exceptions.Timeout as errt:
                    x['headerr']=address+" raised error: Http Error:"+str(errt)
                except requests.exceptions.RequestException as err:
                    x['headerr']=address+" raised error: Http Error:"+str(err)
            
            #TODO: do something about the responses...
            if 'headresponse' in x: 
                if x['readableformat']=='rdfxml':
                    print(resp)
                elif x['readableformat']=='ttl':
                    pass
                elif x['readableformat']=='nq':
                    pass
                elif x['readableformat']=='jsonld':
                    pass
                else:
                    pass

print("#total/#distinct URLs: "+str(cnt)+"/"+str(len(dumpurls)))
print("Guessed formats: "+str([(l,len(formats[l])) for l in formats]))
print("Guessed suffixes: "+str([(l,len(suffixes[l])) for l in suffixes]))

                
with open('lod_datahub.io_enrichted.json', 'w', encoding='UTF-8') as fp:
    json.dump(datasets, fp)


# TODO: Now test whether the guessed format is correct:
# Start with the matched ones:
# use a range request to get enough bytes to see what it is
# if match fails, try other formats, again try most -promising order!

# TODO: move the checking and testing of SPARQL endpoints (previous block after this one!)

