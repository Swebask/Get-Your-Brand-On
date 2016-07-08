# Get-Your-Brand-On
> This is the project I architected during the seven-week <a href="http://insightdataengineering.com/">Insight Data Engineering Fellows Program </a>
which is designed for people with strong knowledge of computer science fundamentals and coding skills to transition to data engineering by giving them a space to get hands-on experience building out distributed platforms on AWS using open source technologies <br/>
Get Your Brand On is an application that harnesses both batch and stream processing to incrementally compute reach and the betweenness centrality scores of nodes in a twitter graph.
You can find the app at <a href="http://swethabaskaran.site> swethabaskaran.site</a>

## Motivation
The <b>Functional </b>motivation for this project was to help build brand awareness by calculating reach of a hashtag/topic related to the brand in real time. Also it would be really interesting to identify the influencial users in the network, who can be potencial brand ambassadors.  <br/> 
The <b>Engineering</b> movitation behind building such a product is to demonstrate the ability to create a scalable big data pipeline to handle graph data and a real-time component.

## Data Pipeline
![alt tag](https://raw.githubusercontent.com/Swebask/Get-Your-Brand-On/master/insightpipeline.png)

<b> Kafka</b> was used as the publish-subscribe broker that ingestes data from the Twitter API.

<b> Spark Streaming </b> was used for Stream Processing over more traditional stream processing systems are designed with a continuous operator model. The reach is calculated in same batches, so Spark Streaming was the logical choice because of it's load balancing and fast failure and straggler recovery capabilities.

<b> Neo4j </b>  was suitable for querying the graph data using cypher. 

<b>Mazerunner</b>(spark-neo4j), a Neo4j unmanaged extension and distributed graph processing platform was deployed using Docker.


