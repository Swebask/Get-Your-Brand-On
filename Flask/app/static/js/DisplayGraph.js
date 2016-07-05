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

