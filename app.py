from flask import Flask
from flask import request
from Aggregate import *

app = Flask(__name__)

@app.route('/')
def baseurl():
	return("working")

@app.route('/mentions/',methods=['GET'])
def mention():
	Search = request.args.get('q')
	return mentions(Search)

if __name__ == '__main__':
	app.run(debug=False,host='0.0.0.0')
