#Batch importing consumed records into neo4j

from py2neo import Graph
import pandas as pd
import random
import json
import urllib2
graph = Graph("http://ec2-52-205-15-39.compute-1.amazonaws.com:7474/db/data")
load_script = """
USING PERIODIC COMMIT 1000
load csv with headers from "http://ec2-52-205-15-39.compute-1.amazonaws.com:8888/tree/kafka.csv" as row
merge (:User {TweetId:row.TweetId,ScreenName:row.ScreenName,FollowerCount:row.FollowerCount})
"""

graph.run(load_script)
df = pd.read_csv('http://ec2-52-205-15-39.compute-1.amazonaws.com:8888/tree/kafka.csv')
userMap = {}
for index, row in df.iterrows():
    userMap[index] = df.iloc[index].ScreenName
def f(x): return userMap[0]
rels = pd.DataFrame()
users=[]
followers=[]
for i in range(50):
    users.append(userMap[random.randint(0,14)])
    followers.append(userMap[random.randint(0,14)]) 
rels['Users'] = filter(f, users)
rels['Followers'] = filter(f, followers)
rels = rels[rels.Users!=rels.Followers]
rels.to_csv('rels.csv',header=True,index=False)
rels_script = """
load csv with headers from "http://ec2-52-205-15-39.compute-1.amazonaws.com:8888/tree/rels.csv" as row2
MATCH (u1:User {ScreenName:row2.Users})
MATCH(u2:User {ScreenName:row2.Followers})
CREATE (u1)-[:FOLLOWEDBY]->(u2)"""
graph.run(rels_script)

url = "http://ec2-52-205-15-39.compute-1.amazonaws.com:7474/service/mazerunner/analysis/betweenness_centrality/FOLLOWEDBY"
res = urllib2.urlopen(url)
print re

node_results = graph.run("MATCH (n:User) RETURN n.ScreenName as ScreenName, n.TweetId as TweetId, n.FollowerCount as FollowerCount")
linksMap = {}
nodes = []
links = []
for index,node_result in enumerate(node_results):
    linksMap[node_result['ScreenName']]=index
    nodes.append({'TweetId': str(node_result['TweetId']), 'ScreenName' : str(node_result['ScreenName']), 'GroupName' : int(node_result['FollowerCount'])%5})

rels_results = graph.run("MATCH (a)-[r:FOLLOWEDBY]-(b) RETURN a.ScreenName as user_ScreenName, b.ScreenName as follower_ScreenName ")
for rels_result in rels_results:
    links.append({'source':linksMap[rels_result['user_ScreenName']],'target':linksMap[rels_result['follower_ScreenName']],'value':random.randint(0,9)})

json_object = {
   "nodes": nodes,
    "links": links
    }
print json.dumps(json_object)