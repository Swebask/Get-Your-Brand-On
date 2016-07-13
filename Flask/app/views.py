from __future__ import print_function
from app import app
from flask.json import jsonify
from flask import Flask, render_template, url_for, request, redirect
from py2neo import Graph
import random
import json
import urllib2
import os
import sys
import yaml
import subprocess
import time,re
import random
import math
from datetime import timedelta,datetime
import pytz
from cassandra.cluster import Cluster

global topic
values = []
#eastern = pytz.timezone('US/Eastern')
os.environ['TZ'] = 'US/Eastern'
time.tzset()

cluster = Cluster(['ec2-52-205-142-108.compute-1.amazonaws.com'])
session = cluster.connect()
session.execute("USE reach;")



@app.route('/')	
@app.route('/index')	
def index():
    return render_template('index.html')

@app.route('/graph', methods= ['GET', 'POST'])
def graph():
	#topic = str(request.args.get('topic'))
	topic = str(request.form['topic'])
	graph = Graph("http://ec2-52-205-15-39.compute-1.amazonaws.com:7474/db/data/")
	node_results = graph.run("MATCH (n:brexit) where has(n.betweenness_centrality) RETURN n.ScreenName as ScreenName, n.TweetId as TweetId, n.FollowerCount as FollowerCount, n.betweenness_centrality as BetweennessCentrality;")
	linksMap = {}
	nodes = []
	links = []
	for index,node_result in enumerate(node_results):
	    linksMap[node_result['ScreenName']]=index
	    nodes.append({'tweetId': str(node_result['TweetId']), 'name' : str(node_result['ScreenName']), 'group' : int(node_result['FollowerCount'])%5,'nodeSize':int(node_result['BetweennessCentrality'])/25+4 })

	rels_results = graph.run("MATCH (a:brexit)-[r:FOLLOWEDBY]-(b:brexit) RETURN a.ScreenName, b.ScreenName")
	for rels_result in rels_results:
		#print(rels_result)
		links.append({'source':linksMap[rels_result['a.ScreenName'].encode('utf-8')],'target':linksMap[rels_result['b.ScreenName'].encode('utf-8')],'value':2})
	json_object = { "nodes": nodes,  "links": links }
	return jsonify(json_object)


@app.route('/stream', methods= ['GET', 'POST'])
def stream():
	start = int(request.args.get('start'))/1000
	stop = int(request.args.get('stop'))/1000
	steps = int(request.args.get('step'))/1000
	start_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
	stop_str  = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stop))
	values = []
	#cons.consume_topic()
	intr = int ( ( stop - start + 1 ) / steps ) + 1
	#print("inter"+str(intr))
	start_date = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
	for n in range(1,intr):
		#print(start_date)
	 	start_date += timedelta(seconds=1)
	 	tempdate = datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S")
	 	#print(tempdate)
	 	rows = session.execute("""SELECT  * from StreamingReach where reachtime = %s""",(str(tempdate),))
	 	for r in rows:
	 		values.append(r.reach)
	return json.dumps(values)


@app.route('/reach', methods= ['GET', 'POST'])
def reach():
	#topic = str(request.args.get('topic'))
	topic = str(request.form['topic'])
	graph = Graph("http://ec2-52-205-15-39.compute-1.amazonaws.com:7474/db/data/")
	graph.run("MATCH (n:"+topic+") SET n.FollowerCount = toInt(n.FollowerCount)")
	reach_count_cursor = graph.run("MATCH (n:"+topic+") RETURN count(n)")
	preach_count_cursor = graph.run("MATCH (n:"+topic+") RETURN sum(n.FollowerCount)")
	for reach_count_record in reach_count_cursor:
		reach_count = reach_count_record[0]
	for preach_count_record in preach_count_cursor:
		preach_count = preach_count_record[0]
	#json_object = { "metric": ["Reach", "Potencial Reach"], "frequency": ["reach_count","reach_count"] }
	json_array = []
	json_array.append({'metric':'Reach', 'value':int(reach_count)})
	json_array.append({'metric':'PotencialReach','value':int(preach_count)})
	json_object = {'records':json_array}

	return jsonify(json_object)



	




