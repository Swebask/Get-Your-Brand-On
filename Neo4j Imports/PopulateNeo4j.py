
# coding: utf-8


from py2neo import Graph
import pandas as pd
import random
import json
import urllib2


# 

with open("neo4jconfig.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

graph = Graph(cfg["graph"]+"/db/data")


# 

load_script = """
USING PERIODIC COMMIT 1000
load csv with headers from %s as row
merge (:brexit {TweetId:row.TweetId,CreatedAt:row.CreatedAt,ScreenName:row.ScreenName,FollowerCount:row.FollowerCount})
"""
graph.run(load_script % (cfg["users"])


# 

rels_script = """
load csv with headers from %s as row2
MATCH (u1:brexit {ScreenName:row2.Users})
MATCH(u2:brexit {ScreenName:row2.Followers})
CREATE (u1)-[:FOLLOWEDBY(]->(u2)"""
graph.run(rels_script % (cfg["relationships"]))


#

url = cfg["graph"]+"/service/mazerunner/analysis/betweenness_centrality/FOLLOWEDBY"
res = urllib2.urlopen(url)

