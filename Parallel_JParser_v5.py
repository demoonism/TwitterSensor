import gzip
import json
import time
#import simplejson as json
import sys
import os
import glob
import datetime
from datetime import datetime
import string
import io
import langid
import nltk
from nltk.corpus import stopwords
from emoji import emoji_dict
#nltk.download("stopwords")
stops = set(stopwords.words("english"))
emoji = emoji_dict

def cleanText(x):
    return not (x.startswith("#")|x.startswith("@")|x.startswith("http:")| (x == '\n')| (x == '\r'))

def getUnixTimeStamp(stamp):
    d = datetime.strptime(stamp,'%a %b %d %H:%M:%S +0000 %Y')
    unixtime = time.mktime(d.timetuple())
    return unixtime

def translating(x):
    return x.encode('utf-8').lower().translate(None, string.punctuation)
    

def onlyEng(r):
    qualified = True
    if (len(r.decode('ascii', 'ignore')) == 0 and (r[:3] not in emoji) and (r[:4] not in emoji)):
        qualified  = False
    return qualified

def parserTerm(term_list):
    
    filtered_list = [word.encode('utf-8').lower().translate(None, string.punctuation) for word in term_list if word not in stops]
    # encode to string, lower case, remove punctuation, remove stop words.
    return filtered_list
    

def parseJson(jsonfile):
    tweets = []

    with gzip.open(jsonfile,'r') as fin:
        for line in fin:

            if len(line) > 1000: # check if the line is proper tweet (by observation, all valid tweets have length at least 1000.)
                try:
                    d = json.loads(line)# more effecient way would be checkvalidity before loading
                    #if len(d['entities']['hashtags']) and d['user']['lang'] == 'en': ## explicit booleaness to check empty
                    # remove last line to include empty hashtags and 
                    ## dow = day of week.  unpacking timestamp into individual vars 
                    ##(or maybe I should do this in the next phase?  note this is still in unicode)
                    #dow, month, day, time,_,year = d['created_at'].split(" ") 
                    ## Assign the extracted values into a new json to be stored.
                    ## UserName --> from_user
                    term_list = parserTerm(d['text'].strip('').split(' '))
                    #clean_list = list(filter(cleanText, term_list))
                    #better_list = list(map(translating,term_list))
                    final_list = list(filter(onlyEng,term_list))
                    if d['user']['location'] == None:
                        loc_term = ""
                    else:
                        loc_term = 'loc_' + "_".join(map(translating, d['user']['location'].split(" ")))

                    if d['text'] == None:
                        terms = ""
                    else:
                        terms = " ".join(final_list)        

                    if len(terms):
                        processed = {"from_user":d['user']['screen_name'],
                                     "from_id":d['user']['id'],
                                     ## Split hashtag, we only want the text in hashtag, discard indices.
                                     "tweet_id":d['id'],
                                     "hashtag":" ".join([hash_string['text'] for hash_string in d['entities']['hashtags']]), 
                                     ## Split terms in tweet text, remove \n and \r
                                     "term": terms, 
                                     ## append loc_ to each word in location
                                     #"location":['loc_' + s for s in d['user']['location'].split(" ")],
                                     "location":loc_term,
                                     ## mention ids
                                     "mention":" ".join([mention['screen_name'] for mention in d['entities']['user_mentions']]),
                                     "HashTag_Birthday":getUnixTimeStamp(d['created_at'])
                                    }
                        tweets.append(processed)
                        
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
        for eachjson in output:
            json.dump(eachjson, f) # only works for json, cannot write(json)
            f.write('\n')
            
def getTime(start):
    sec = time.time() - start
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return s,m,h, sec

if __name__ == "__main__":

    #print(sys.argv)
    start_time = time.time()

    ## Linux
    #jsonfilename = '/home/danielshi/tw_pyspark/Sample/statuses.log.2014-07-01-00.gz'
    input_arg = sys.argv[1]    #'/mnt/f/Demoonism/Data Scientist/Spark/statuses.log.2014-07-01-00.gz'
    output_arg = '/mnt/66e695cd-1a0c-4e3b-9a50-55e01b788529/Tweet_Output/All' 
    # /mnt/1e69d2b1-91a9-473c-a164-db90daf43a3d/Backup_tw_2013_2_8/2013-02/2013-02-01
    print("running folder ",input_arg, "..." )
    ## Windows
    #jsonfilename = '../statuses.log.2014-07-01-00.gz'
    
    counter = 0
    
    for item in os.listdir(input_arg):
        for dayfile in os.listdir(os.path.join(input_arg, item)):
            now = time.time() ## timer set!

            folder = os.path.join(input_arg, item, dayfile)
            path = os.path.join(folder,'*.gz')
            files = glob.glob(path)
            out_filename = os.path.join(output_arg, dayfile)

            output = []
            print('Total of ', len(files),' files')
            for jsonfilename in files:

                result = parseJson(jsonfilename)
                output = output + result  ##simple list concatenation
                counter += 1
                if counter % 5 == 0:
                    print "processed ", counter


            s,m,h,sec = getTime(now)
            print('Pre-processing '+ dayfile +' takes - %d:%02d:%02d which is %d seconds in total' % (h,m,s,sec))

            toGizpJson(out_filename)
                
    s,m,h,sec = getTime(start_time)
    print('Finally....Pre-processing the entire folder takes - %d:%02d:%02d which is %d seconds in total' % (h,m,s,sec))                