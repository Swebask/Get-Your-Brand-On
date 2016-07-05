

function renderTimeSeries(graph) {

var context = cubism.context().step(1 * 1000).size(960)
var horizon = context.horizon().extent([0,100])
  
// draw graph
var metrics = ["Reach"]
horizon.metric(random_ma(context,horizon));
 
d3.select("#graph").selectAll(".horizon")
      .data(metrics)
      .enter()
      .append("div")
      .attr("class", "horizon")
      .call(horizon);
 
// set rule
d3.select("#timeWidget").append("div")
  .attr("class", "rule")
  .call(context.rule());
 
// set focus
context.on("focus", function(i) {
    d3.selectAll(".value")
        .style( "right", i == null ? null : context.size() - i + "px");
});
// set axis 
var axis = context.axis()
d3.select("#graph").append("div").attr("class", "axis").append("g").call(axis);

}

// define metric accessor
function random_ma(context,horizon) {
    return context.metric( function(start, stop, step, callback) {
        var values =[];
        d3.json("/stream?start="+ +start+"&stop="+ +stop+"&step="+ +step, function(rows) {
            rows.forEach(function(d) {
                values.push(d);
            });
            callback(null, values);
        });
    }, name);
};