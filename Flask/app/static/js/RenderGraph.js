// Force Directed Graph to show the reach network of a topic or hashtag

function renderGraph(graph) {
  var width = 960,
      height = 500;

  var color = d3.scale.category20();

  var zoom = d3.behavior.zoom()
    .scaleExtent([0.1, 7])
    .on("zoom", redraw);

  var force = d3.layout.force()
  .charge(-150)
  .linkDistance(70)      
  .friction(0.7)
  .size([width, height]);

  var svg = d3.select("#graphWidget").append("svg")
  .attr("width", width)
  .attr("height", height)
  .call(zoom);

  force
    .nodes(graph.nodes)
    .links(graph.links)
    .start();

  var link = svg.selectAll(".link")
  .data(graph.links)
  .enter().append("line")
  .attr("class", "link")
  .style("stroke-width", function(d) {
    return Math.sqrt(d.value);
  });

  var node = svg.selectAll(".node")
  .data(graph.nodes)
  .enter().append("circle")
  .attr("class", "node")
  .attr("r", function(d) {
    return d.nodeSize;
  })
  .style("fill", function(d) {
    return color(d.group);
  })
  .on("click", nodeClick)
  .on("mouseover", displayTweet)
  .call(force.drag);

  node.append("title")
    .text(function(d) {
    return d.name;
  });

  force.on("tick", function() {

    link.attr("x1", function(d) {
      return d.source.x;
    })
      .attr("y1", function(d) {
      return d.source.y;
    })
      .attr("x2", function(d) {
      return d.target.x;
    })
      .attr("y2", function(d) {
      return d.target.y;
    });

    node.attr("cx", function(d) {
      return d.x;
    })
      .attr("cy", function(d) {
      return d.y;
    });
  });

  function redraw() {
      svg.attr("transform",
          "translate(" + d3.event.translate + ")"
          + " scale(" + d3.event.scale + ")");  
  }

}
