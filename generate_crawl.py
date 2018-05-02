from collections import defaultdict
import requests
import sys
import json
import simplejson


# uncomment to load datasets from a local file
with open('/Users/apollere/Documents/software/my_github_forks/LODanalysis/lod_datahub.io_enrichted.json', 'r', encoding='UTF-8') as fp:
    datasets=json.load(fp)

# createcsv=False
createcsv=True
if createcsv==True:
    print('"curl";"address";"bio";"guessedsparql";"id";"guessedsuffix"')

# Find out which datasets in the LOD cloud fulfill the following condition:
# The dataset must contain at least 1000 triples. (Hence, your FOAF file most likely does not qualify.)
    
rcnt=0
dcnt=0
drdcnt=0
hcnt=0
biocnt=0
address=""
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
        # if bio == False: break
        for x in i['resources']:
            rcnt = rcnt+1
            address = x['url'].strip()
            s=address.lower()
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
            #print(address)
            #TODO: do something about the responses...
            if 'headresponse' in x: 
                hcnt = hcnt+1
                dhcnt = dhcnt+1
                if x['guessedsparql'] == False:
                    if createcsv==True:
                        print('curl "'+s+'" -o '+x['id']+';"'+address+'";'+str(bio)+';'+str(x['guessedsparql'])+';'+x['id']+';"'+x['guessedsuffix']+'"')

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
    print("i.e., "+ str(dcnt-drdcnt)+" dataset descriptions contain no dereferenceable resource URLs (not counting links to HTML, PDF, CSV, TSV, though).")
    print("Admittedly, the HTMLs *could* contain RDFa or links to RDF, and HTMLs *could* also have links to actual RDF/LinkedData content, but this is not very machine-friendly, as it would need to do a site crawl.")
    print("Among all datasets "+str(biocnt)+" have tags that hint to the Biomedical and LifeSciences domain")
    print("Guessed formats: "+str([(l,len(formats[l])) for l in formats]))
    print("Guessed suffixes: "+str([(l,len(suffixes[l])) for l in suffixes]))

