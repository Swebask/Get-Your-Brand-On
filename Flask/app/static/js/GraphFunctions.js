// Opens a new tab with the corresponding user's twitter page when a node of the force directed graph is clicked
function nodeClick(a) {
    var tweetUrl = "http://twitter.com/" + a.name + "/status/" + a.tweetId;
    window.open(tweetUrl, '_blank');
    console.log("in")
}

// Display corresponding user's tweet when a node of the force directed graph is hovered on
function displayTweet(a) {
    removeTweet(a);
    var tweetWidget = document.getElementById("tweetWidget");
    twttr.widgets.createTweet(
        a.tweetId, tweetWidget, {
            conversation: 'none', // or all
            cards: 'hidden', // or visible 
            linkColor: '#cc0000', // default is blue
            theme: 'light' // or dark
        })
}

// Empty the 'tweetWidget' div element when not hovering on a node
function removeTweet(a) {
    $("#tweetWidget").empty()

}