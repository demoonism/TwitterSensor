
import gzip
import json
import time
import sys
import os
import glob
import string
import io
import datetime
from datetime import datetime

import langid

langid.set_languages(['de','fr','it','en','zh','ar','ja','ko', 'es','ms','tr','hi','bn','pa'])

# https://www.loc.gov/standards/iso639-2/php/code_list.php
# https://github.com/saffsd/langid.py/blob/master/README.rst


def getTime(start):
    sec = time.time() - start
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return s,m,h, sec


loading = time.time()
print(langid.classify(u"nihatkahveci8 abi ingiltereye 2 0 yenildi\\u011fimiz ma\\u00e7taki kafa "))
getTime(loading)


def FilterJson(jsonfile):
    tweets = []

    with gzip.open(jsonfile,'r') as fin:
        for line in fin:
            try:
                d = json.loads(line)# more effecient way would be checkvalidity before loading
                if(langid.classify(d['term'])[0] == 'en'):
                    tweets.append(d)

            except ValueError as e:
                print("one line is not properly formated!")
                with open('amomaly.json', 'wb') as k:
                    #json.dump(line, f) # only works for json, cannot write(json)
                    k.write(line)
                    k.write('\n')
            except Exception:
                pass
                #print(d['text'])
                #print("invalid text.....definitly not english")                
    return tweets


def toGizpJson(filename):
    with gzip.open(filename+'.json.gz', 'wb') as f:
        for eachjson in result:
            json.dump(eachjson, f) # only works for json, cannot write(json)
            f.write('\n')


if __name__ == "__main__":

    #print(sys.argv)
    start_time = time.time()

    ## Linux
    #jsonfilename = '/home/danielshi/tw_pyspark/Sample/statuses.log.2014-07-01-00.gz'
    input_arg = sys.argv[1]    #'/mnt/f/Demoonism/Data Scientist/Spark/statuses.log.2014-07-01-00.gz'
    output_folder = '/mnt/66e695cd-1a0c-4e3b-9a50-55e01b788529/Tweet_Output/All_Eng/' 
    # /mnt/1e69d2b1-91a9-473c-a164-db90daf43a3d/Backup_tw_2013_2_8/2013-02/2013-02-01
    #print("running folder ",input_arg, "..." )
    ## Windows
    #jsonfilename = '../statuses.log.2014-07-01-00.gz'
    #Preprocessed_path = '/mnt/66e695cd-1a0c-4e3b-9a50-55e01b788529/Tweet_Output/small_sample'
    counter = 0
    
    now = time.time() ## timer set!

    path = os.path.join(input_arg,'*.gz')
    files = glob.glob(path)

    output = []
    print("Working on", input_arg)
    print('Total of ', len(files),' files')
    for jsonfilename in files:
        result = FilterJson(jsonfilename)
        
        dayfile = os.path.basename(jsonfilename)        
        out_path = os.path.join(output_folder,"clean-"+dayfile)
        
        toGizpJson(out_path)
        
        counter += 1
        if counter % 5 == 0:
            print "processed ", counter," day files"
        


    s,m,h, sec = getTime(now)
    print('English Filtering done, process takes %d:%02d:%02d which is %d seconds in total' % (h,m,s,sec))



