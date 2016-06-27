import re
from collections import Counter
import functools

with open('enronemail_1h.txt','r') as file:
    lines=file.readlines() 


def returnCounter(lineObject):
    line = re.split("[^A-Za-z]", lineObject.strip())
    englishWords=[w for w in line if len(w)]
    Cntr=Counter()
    for word in englishWords:
        Cntr[word]+=1
    return Cntr


def reducer(mappedObject):
    return functools.reduce(lambda counterObjectX, counterObjectY :counterObjectX + counterObjectY, mappedObject)


#TotalCounterObject=reducer(mapper(lines))

TotalCounterObject=reducer(map(returnCounter, lines))

#Print occurence of word assistance
print TotalCounterObject['assistance']
    
#print top ten words
print TotalCounterObject.most_common()[0:9]
    
    
    
    
    
    
    
