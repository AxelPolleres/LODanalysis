from collections import defaultdict
import requests
import sys
import json
import simplejson
from SPARQLWrapper import SPARQLWrapper, JSON


# uncomment to load datasets from a local file
with open('lod_datahub.io_enrichted.json', 'r', encoding='UTF-8') as fp:
    datasets=json.load(fp)

createcsv=True
#createcsv=False
#if createcsv==True:
#    print('"curl";"address";id;"bio";"guessedsparql";"guesseddum";"guessedsitemap";"guessedvoid";"guessedsuffix"')

# Find out which datasets in the LOD cloud fulfill the following condition:
# The dataset must contain at least 1000 triples. (Hence, your FOAF file most likely does not qualify.)
    
rcnt=0
dcnt=0
drdcnt=0
hcnt=0
biocnt=0
address=""

qask = 'ASK {?S ?P ?O . }'
qask1 = 'ASK {}'
qask2 = 'ASK {GRAPH ?G {?S ?P ?O . }}'
qask3 = 'ASK {GRAPH ?G {}}'
qlookup = 'SELECT ?X WHERE { ?X <http://www.example.org> <http://www.example.org> . }'
qtriplecount = 'SELECT (count(*) AS ?C) WHERE  {?S ?P ?O . }'
qquadcount = 'SELECT (count(*) AS ?C) WHERE  { GRAPH ?G { ?S ?P ?O . }}'

qask_enc = 'query=ASK+%7B%3FS+%3FP+%3FO+.+%7D'
qask1_enc = 'query=ASK+%7B%7D'
qask2_enc = 'query=ASK+%7BGRAPH+%3FG+%7B%3FS+%3FP+%3FO+.+%7D%7D'
qask3_enc = 'query=ASK+%7BGRAPH+%3FG+%7B%7D%7D'
qlookup_enc = 'query=SELECT+%3FX+WHERE+%7B+%3FX+%3Chttp%3A%2F%2Fwww.example.org%3E+%3Chttp%3A%2F%2Fwww.example.org%3E+%7D'
qtriplecount_enc = 'query=SELECT+(count(*)+AS+%3FC)+WHERE+%7B%3FS+%3FP+%3FO%7D'
qquadcount_enc = 'query=SELECT+(count(*)+AS+%3FC)+WHERE+%7BGRAPH+%3FG+%7B%3FS+%3FP+%3FO%7D%7D'

formats = defaultdict(list)
suffixes = defaultdict(list)
suffix_or_format = defaultdict(list)

for ds in datasets:
    for i in ds['results']:
        dhcnt=0
        dcnt=dcnt+1
        bio=False
        if 'tags' in i:
            for tag in i['tags']:
                if any(substring in tag['name'] for substring in ['bio', 'life', 'medic', 'clinic']) :
                    #print(tag['name'])
                    biocnt=biocnt+1
                    bio=True
        for x in i['resources']:
            rcnt = rcnt+1
            address = x['url'].strip()
            s=address.lower()
            suffix='other'
            if 'guessedsuffix' in x:
                suffixes[x['guessedsuffix']].append(s)
                if x['guessedsuffix'] not in  ['othersuffix','nosuffix']:
                    suffix = x['guessedsuffix']
            format = "other"
            if 'guessedformat' in x:
                f=x['format'].lower()
                formats[x['guessedformat']].append(f)
                if x['guessedformat'] != 'otherformat':
                    format = x['guessedformat']


            #Does the address contain the string "sparql"? 
            if suffix != 'other':
                suffix_or_format[suffix].append(str(i))
            else:
                suffix_or_format[format].append(str(i))

            if 'sparql' in s:
                x['guessedsparql'] = True
            else:
                x['guessedsparql'] = False
                
            #Does the address contain the string "dump"? 
            if ('dump' in s) or ('download' in s):
                x['guesseddump'] = True
            else:
                x['guesseddump'] = False

            if ('sitemap.xml' in s):
                x['guessedsitemap'] = True
            else:
                x['guessedsitemap'] = False

            if ('void' in s):
                x['guessedvoid'] = True
            else:
                x['guessedvoid'] = False
            
            #print(address)
            #TODO: do something about the responses...

            if 'headresponse' in x:
                hcnt = hcnt+1
                dhcnt = dhcnt+1
                if createcsv==True:
                    curl=""
                    if x['guessedsparql'] and not x['guessedsitemap'] and not x['guesseddump']and x['guessedsuffix'] not in ['ttl','trig','nt']:
                        # this is mainly to address a strenage URL that has a space in it...
                        address = address.replace(" ", "")
                        address = address.split('?')[0]
                        if (address.endswith('.html') or address.endswith('.tpl')):
                            address = address[0:address.rfind('/')+1]+'sparql'
                        # strip off everything after "?" (some urls have already queries encoded)
                        if not ( address.endswith('sparql') or address.endswith('sparql/')):
                            if not address.endswith('/'):
                                address = address + '/'
                            address = address+'sparql'

                        # TODO the preferred headers could be configured based on guessed format/suffix"!!!
                        # ... at the moment I just try to get RDF if I can
                        q="curl -L "+'"'+address+'?'
                        curl=q+qask_enc+'" -o '+x['id']+'_sparql_qask'
                        print(curl)
                        curl=q+qask1_enc+'" -o '+x['id']+'_sparql_qask1'
                        print(curl)
                        curl=q+qask2_enc+'" -o '+x['id']+'_sparql_qask2'
                        print(curl)
                        curl=q+qask3_enc+'" -o '+x['id']+'_sparql_qask3'
                        print(curl)
                        curl = q+qlookup_enc+ '" -o ' + x['id'] + '_sparql_qlookup'
                        print(curl)
                        curl = q + qtriplecount_enc + '" -o ' + x['id'] + '_sparql_qtriplecount'
                        print(curl)
                        curl = q + qquadcount_enc + '" -o ' + x['id'] + '_sparql_qquadcount'
                        print(curl)

                    curl="curl -L -H 'Accept: text/turtle, application/n-triples, application/trig, application/n-quads, application/rdf+xml, *'"+' "'+address+'" -o '+x['id']
                    print(curl)
                    # if bio:
                    #    print(curl)

                    # TODO:
                    print(curl+';"'+address+'";'+x['id']+';'+str(bio)+';'+str(x['guessedsparql'])+';'+str(x['guesseddump'])+';'+str(x['guessedsitemap'])+';'+str(x['guessedvoid'])+';"'+x['guessedsuffix']+'"')


                # The dataset must contain at least 1000 triples. (Hence, your FOAF file most likely does not qualify.)
                    #if x['readableformat'] in ['rdfxml','hdt','ttl','nq','trig','jsonld']:
                    #    if createcsv==True:
                    #        print('curl "'+s+'" -o '+x['id']+';"'+address+'";'+x['id']+';"'+x['guessedsuffix']+'"')
                    #    pass
                    #    # AT THE MOMENT ONLY CRAWL THOSE WHICH WE ARE PRETTY SURE ABOUT BEING RDF:
                    #else:
                    #    if createcsv==True:
                    #        print('curl "'+s+'" -o '+x['id']+';"'+address+'";'+x['id']+';"'+x['guessedsuffix']+'"')
                    #    pass
                      
                sys.stdout.flush()


        if dhcnt>0:
            drdcnt=drdcnt+1

if createcsv==False:
    print("#ressources/#datasets/#non-err-ressources/#datasetswithnon-err-ressources: "+str(rcnt)+"/"+str(dcnt)+"/"+str(hcnt)+"/"+str(drdcnt))
    print("This means that among the dereferenced "+str(rcnt)+" resources in the "+str(dcnt)+" datasets of the LOD cloud on datahub.io")
    print("there are only "+str(hcnt)+" resources URLs could be dereferenced.")
    print("Among those, some TODO are just examples rdf files with less than 1000 triples, which is the lower limit")
    print("according to http://lod-cloud.net/#about")
    print("That is, "+ str(drdcnt)+" dataset descriptions contain dereferenceable resource URLs;")
    print("i.e., "+ str(dcnt-drdcnt)+" dataset descriptions contain no dereferenceable resource URLs (not counting links to PDF, CSV, TSV files).")
    print("Admittedly, the HTMLs *could* contain RDFa or links to RDF, and HTMLs *could* also have links to actual RDF/LinkedData content, but this is not very machine-friendly, as it would need to do a site crawl.")
    print("Among all datasets "+str(biocnt)+" have tags that hint to the Biomedical and LifeSciences domain")
    print("Guessed formats: "+str([(l,len(formats[l])) for l in formats]))
    print("Guessed suffixes: "+str([(l,len(suffixes[l])) for l in suffixes]))
    print("Guessed formats or suffixes aggregated:")
    for l in suffix_or_format:
       print(str(l) + ';' + str(len(suffix_or_format[l])))

