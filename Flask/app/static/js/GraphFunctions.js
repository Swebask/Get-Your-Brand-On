function nodeClick(a) {
    var tweetUrl = "http://twitter.com/" + a.name + "/status/" + a.tweetId;
    window.open(tweetUrl, '_blank');
    console.log("in")
}

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

function removeTweet(a) {
    $("#tweetWidget").empty()

}