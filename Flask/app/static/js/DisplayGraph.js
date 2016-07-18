// Makes POST requests for the reach time series and force directed graph data for a topic or hashtag

function displayGraph() {

  var topic = $('input[name=topic]').val()
  console.log(topic)
  $.ajax({
    type: 'POST',
    url: '/graph',
    data: {
      topic: topic
    },
    success: function(data) {
    renderGraph(data)
  },
    error: function(textStatus, errorThrown) {
      console.log(textStatus)
    }

  })

  $.ajax({
    type: 'POST',
    url: '/reach',
    data: {
      topic: topic
    },
    success: function(data) {
    reachStats(data)
  },
    error: function(textStatus, errorThrown) {
      console.log(textStatus)
    }

  })

  renderTimeSeries();
}

