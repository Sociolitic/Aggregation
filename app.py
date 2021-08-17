from flask import Flask
from flask import request
from Aggreate import *

app = Flask(__name__)

@app.route('/')
def baseurl():
	return("working")

@app.route('/mentions/',methods=['GET'])
def mention():
	Search = request.args.get('q')
	return mentions_count(Search)

@app.route('/sentiment_mentions/',methods=['GET'])
def sentiment_mention():
	Search = request.args.get('q')
	Sentiment = request.args.get('sentiment')
	return sentiment_count(Search,Sentiment)

if __name__ == '__main__':
	app.run(debug=False,host='0.0.0.0')
