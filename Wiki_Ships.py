'''
Created on Jan 21, 2017

'''
__author__ = "Michael Huber"
__authors__ = ""
__email__ = "michael.huber@stud-mail.uni-wuerzburg.de"
__version__ = "0.1"
__date__ = "2017-02-13"

import json
from urllib.parse import urlencode, unquote_plus
from urllib.request import urlopen
import mwparserfromhell
import pandas as pd
import regex as re
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import matplotlib.cm as cmx
import numpy as np
from collections import defaultdict
import requests


               

def queryreqWikidata(sparqlquery):
    '''sends a SPARQL-query to Wikidata-API

    Args:
        query(str):SPARQL-Query 

    Returns:
        pd.DataFrame: pandas Dataframe containing the results of 'query' sent to Wikidata-API
    
    
    '''
        
    
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    data = requests.get(url, params={'query': query, 'format': 'json'}).json()
    print(data)
    ships = []
    for item in data['results']['bindings']:
        ship = {}
        for columnhead in data['head']['vars']:
            if columnhead in item:
                ship[columnhead] = item[columnhead]['value']             
        ships.append(ship)

    df = pd.DataFrame(ships)
    df.head()
    
    return df



def scrapeWiki(title, language = "en"):
    """Scrapes Wikipediapage using MediaWiki-API

    Args:
        title (str): title of Wikipediapage.
        language (str): ISO 693-1.language code to address a specific Wikipedia. language defaults to "en"

    Returns:
        mwparserfromhell.Wikicode object: string object with additional methods
    """
    
    API_URL = 'https://' + language + '.wikipedia.org/w/api.php'
    data = {"action": "query", "prop": "revisions", "rvlimit": 1,
            "rvprop": "content", "format": "json", "titles": unquote_plus(title)}
    raw = urlopen(API_URL, urlencode(data).encode())
    #print(type(raw))
    raw = raw.read().decode('utf-8')
    #print(type(raw))
    res = json.loads(raw)
    temp=res["query"]["pages"].keys()
    pageid =''
    for item in temp:
        #print (item)
        pageid = item
    temp = res["query"]["pages"]
    #print (temp)
    #print(temp[0])
    text = res["query"]["pages"][pageid]["revisions"][0]["*"]
    #print (text)
    
    wikicodetext = mwparserfromhell.parse(text)
    
    return wikicodetext

def parseWikipageForInfobox(wikipageTitle,attributeList,infoboxlevel):
    """Parses a mwparserfromhell.Wikicode object scraped from Wikipedia for given List of Infobox attributes

    Args:
        wikipageTitle(str): title of Wikipedia page.
        attributeList [str]: attributes listed in infobox of Wikipedia page
        infoboxlevel(str): name of the infobox template covering the searched information

    Returns:
        dict{str:str}: Dictionary with attribute:value pairs of attributeList
        
    Notes:
        To Do: parse infobox recursively
    """
    
    attValdict= {}
    for template in scrapeWiki(wikipageTitle).filter_templates():
        if template.name.matches(infoboxlevel):#possibility to add 'Infobox ship characteristics'
            for attribute in attributeList:#make list comprehension
                if template.has(attribute) and template.get(attribute).value.rstrip('\n'):#implement try -else
                    attValdict[attribute.replace(" ","_")]= template.get(attribute).value.rstrip('\n')
                    
    
    return attValdict



def createdf(sparqldf, attributeList=["Ship laid down","Ship ordered","Ship launched","Ship christened","Ship completed","Ship fate","Ship status", "Ship builder"],infoboxlevel="Infobox ship career"):
    """appends a previously created csv-file by parsed attributes from parseWikipageForInfobox()

    Args:
        sparqldf(pd.DataFrame): previously created DataFrame from SPARQL-query.
        attributeList [str]: attributes listed in infobox of Wikipedia page. defaults to: "Ship laid down","Ship ordered","Ship launched","Ship christened","Ship completed","Ship fate","Ship status", "Ship builder"

    Returns:
        pd.DataFrame: DataFrame consisting of results of SPARQL-query + parsed Infobox
    """
    
    for index, row in sparqldf.iterrows():
        
        if isinstance(row['sitelink'], str):
            #print(row['sitelink'].split('/')[-1])
            title = row['sitelink'].split('/')[-1]
        else:
            continue
        #print(title)
        infoboxdict = parseWikipageForInfobox(title, attributeList,infoboxlevel)
        for attribute, value in infoboxdict.items():
            #print("key: ", attribute, "value: ",value)
            if attribute in sparqldf.columns:
                sparqldf.ix[index, attribute] = value
                #print(index, "|",attribute,"|",value)
            else:
                sparqldf[attribute]=""
                sparqldf.ix[index, attribute]=value
                #print(attribute,":-Attribute set")
    return sparqldf

def createCSV(df,filePath):
    """saves a pd.DataFrame as csv-file 

    Args:
        df(pd.DataFrame): any valid pd.DataFrame
        filePath(str): file path to csv
    Returns:
        
    """
    df.to_csv(filePath)


def normalizeDate(df):
    """normalises dates from SPARQL-query or Infobox and saves it in extra column 

    Args:
        df(pd.DataFrame): pd.DataFrame with information parsed from Wikipedia infobox ships-Template
        
    Returns:
        pd.DataFrame: DataFrame consisting of results of SPARQL-query, parsed Infobox and normalised date
        
    """    
    
    expr = re.compile('\d\d\d\d')
    df['normalized_date']=None
    for index, row in df.iterrows():
        
        if isinstance(row['Ship_laid_down'], str):
            if re.search(expr, row['Ship_laid_down']):
                date = re.search(expr, row['Ship_laid_down']).group()
                print(date)
                df.ix[index, 'normalized_date'] = date
                continue
        elif isinstance(row['Ship_ordered'], str):
            if re.search(expr, row['Ship_ordered']):
                date = re.search(expr, row['Ship_ordered']).group()
                print(date)
                df.ix[index, 'normalized_date'] = date
                continue 
        elif isinstance(row['Ship_launched'], str):
            if re.search(expr, row['Ship_launched']):
                date = re.search(expr, row['Ship_launched']).group()
                print(date)
                df.ix[index, 'normalized_date'] = date
                continue
        elif isinstance(row['Ship_completed'], str):
            if re.search(expr, row['Ship_completed']):
                date = re.search(expr, row['Ship_completed']).group()
                print(date)
                df.ix[index, 'normalized_date'] = date
                continue
        elif isinstance(row['Ship_christened'], str):
            if re.search(expr, row['Ship_christened']):
                date = re.search(expr, row['Ship_christened']).group()
                print(date)
                df.ix[index, 'normalized_date'] = date
                continue
        else:
            continue
    
    return df

def normalizeManufacturer(df):
    """normalises parsed string from Infobox about manufacturer and saves it in extra column 

    Args:
        df(pd.DataFrame): pd.DataFrame with information parsed from Wikipedia infobox ships-Template
        
    Returns:
        pd.DataFrame: DataFrame consisting of results of SPARQL-query, parsed Infobox, normalised date and normalised manufacturer
        
    """        

    expr0 =re.compile('\|.*?(?=]])')
    expr1 =re.compile(',.*')
    expr2 =re.compile('\(.*\)')
    #expr3 =re.compile('<.*>')
    df['normalized_manufacturer']=None
    
    for index, row in df.iterrows():
        if isinstance(row['manufacturerLabel'],str):
            df.ix[index, 'normalized_manufacturer'] = row['manufacturerLabel']

            continue
        elif isinstance(row['Ship_builder'],str):
            builder = row['Ship_builder']
            
            df.ix[index, 'normalized_manufacturer'] = re.sub(expr2, '', re.sub(expr2, '',re.sub(expr1, '', re.sub(expr0, '', builder, re.MULTILINE)).translate(str.maketrans('', '', '[]*')))).lstrip()
            
            
        else:
            continue
    return df
        
    
def createVisDict(df, starttime=1840, endtime=1883):
    """creates a nested Dictionary with information for further visualization 

    Args:
        df(pd.DataFrame): pd.DataFrame with information parsed from Wikipedia infobox ships-Template including normalised date and manufacturer
        starttime(int): starting point of time frame for visualization
        endttime(int): end point of time frame for visualization
    Returns:
        dict(manufacturer:{year:count}): Dicionary with dictionaries consisting of "year, amount of ships build" - pairs
        
    """     
    
    wikidf = df.query(str(starttime) + ' <= normalized_date < ' + str(endtime))
    visDict = defaultdict(lambda: {val:0 for val in range(starttime,endtime)})
    for index,row in wikidf.iterrows():
        if isinstance(row['normalized_manufacturer'], str):
            manufacturer = row['normalized_manufacturer']
            year = row['normalized_date']
            visDict[manufacturer][int(year)]+=1

    
    return visDict, starttime, endtime   

def createColoredBarplot(visDict):
    """plots amount of ships build x year in a bar plot 

    Args:
        df(pd.DataFrame): pd.DataFrame with information parsed from Wikipedia infobox ships-Template
        
    Returns:
        pd.DataFrame: DataFrame consisting of results of SPARQL-query, parsed Infobox, normalised date and normalised manufacturer
        
    """ 
  


    xax=np.arange(visDict[1],visDict[2])
    width = 0.8
    stackbottom = np.zeros(visDict[2]-visDict[1])

    

            
    listforcolormap = list(range(len(visDict[0].keys())))
    cNorm = colors.Normalize(vmin=0, vmax=max(listforcolormap), clip=True)
    jet = plt.get_cmap('jet')
    scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=jet)
    
    for colorcount, manufacturer in enumerate(visDict[0]):
    
        countList=np.array(list(visDict[0][manufacturer].values()))
        colorVal = scalarMap.to_rgba(colorcount)
        plt.bar(xax, countList, width,bottom=stackbottom, color=colorVal, label=manufacturer)
        stackbottom += countList
        
    plt.xlabel('Years')
    plt.ylabel('Number of ships constructed')
    plt.title('Amount of ships constructed per manufacturer per year')
    plt.legend()
    plt.show()
    
def createBarplot(visDict):
    """plots amount of ships build x year in a bar plot 

    Args:
        visDict: 3-touple with dictionary, starttime and endtime passed by createVisDict()
        
        
    Returns:
        barplot
        
    """ 

    xax=np.arange(visDict[1],visDict[2])
    width = 0.8
    countList = np.zeros(visDict[2]-visDict[1])      
    
    for manufacturer in visDict[0]:

        countList1=np.array(list(visDict[0][manufacturer].values()))
        countList =countList + countList1

    plt.bar(xax, countList, width)
        
    plt.xlabel('Years')
    plt.ylabel('Number of ships constructed')
    plt.title('WikiShips simple barplot')
    plt.legend()
    plt.show()

query = '''PREFIX wd: <http://www.wikidata.org/entity/> 
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                
                SELECT DISTINCT ?ship ?shipLabel ?manufacturerLabel ?sitelink
                WHERE {
                  ?ship wdt:P137 wd:Q172771 .
                
                  OPTIONAL{
                    ?ship wdt:P176 ?manufacturer.
                          }
                  OPTIONAL{
                    ?sitelink schema:about ?ship.
                    ?sitelink schema:isPartOf <https://en.wikipedia.org/>  
                  }    
                FILTER NOT EXISTS{
                    ?ship wdt:P31 wd:Q559026.  
                            }
                FILTER NOT EXISTS{
                    ?ship wdt:P279* wd:Q32050.
                            }
                FILTER NOT EXISTS{
                    ?ship wdt:P279* wd:Q19623198
                            }             
                SERVICE wikibase:label {
                    bd:serviceParam wikibase:language "en" .
                   }
                }'''



#the following functions showcase how the script works, step-by-step
#to avoid writing the output you can simply nest the functions

#sends a query to the wikidata-api and saves the result
createCSV(queryreqWikidata(query), '/Users/MHuber/Documents/WS1617/Wikiships/queryviapython.csv')
#access the wikipediapage saved in the column sitelink and parses the infobox for information (defaults to Ship laid down","Ship ordered"...)
createCSV(createdf(pd.read_csv('/Users/MHuber/Documents/WS1617/Wikiships/queryviapython.csv')), '/Users/MHuber/Documents/WS1617/Wikiships/ships_completed.csv')
#normalizes the parsed information and the wikidata query results of manufacturer and year
createCSV(normalizeManufacturer(normalizeDate(pd.read_csv('/Users/MHuber/Documents/WS1617/Wikiships/ships_completed.csv'))),'/Users/MHuber/Documents/WS1617/Wikiships/manufacturers_dates_normalized.csv' )
#creates a simple barplot which shows how many ships were build during a time period
createBarplot(createVisDict(pd.read_csv('/Users/MHuber/Documents/WS1617/Wikiships/manufacturers_dates_normalized.csv')))

#the colorod Barplot looses its viability when the timespan is to long, therefore the additional argument limits the amount of data.
#createColoredBarplot(createVisDict(pd.read_csv('/Users/MHuber/Documents/WS1617/Wikiships/manufacturers_dates_normalized.csv'), 1860, 1865))

#this function showcases how the information parsed in the infoboxes can be adjusted. Sadly there is no visualization for it yet
#createCSV(createdf(pd.read_csv('/Users/MHuber/Documents/WS1617/Wikiships/manufacturers_dates_normalized.csv' ), ['Ship displacement', 'Ship length', 'Ship speed'], "Infobox ship characteristics"), '/Users/MHuber/Documents/WS1617/Wikiships/ships_test_lengthtonnage_normalized.csv' )    






print("FINISHED")



