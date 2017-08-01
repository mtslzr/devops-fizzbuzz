import boto3
import time
from datetime import datetime

# Function - Main Handler
def handler(event, context):
	# Start our class.
	fb = Fizzbuzz()

	# If ID, we're getting an existing run.
	if 'id' in event['params']['path']:
		return fb.getFizzbuzz(event['params']['path']['id'])
	# Else, we're creating a new entry.
	else:
		# Setup data and ensure we're using numbers.
		data = {}
		for key,val in event['params']['path'].iteritems():
			try:
				int(val)
				data[key] = int(val)
			except:
				return val + " is not an Integer."

		if data['x'] == 0 or data['y'] == 0:
			return "Stop trying to divide by zero. :("

		# Set default data, if needed. 
		if len(data) == 0:
			data = { 'x': 3, 'y': 5 }
		if not 'z' in data:
			data['z'] = 100

		return fb.runFizzbuzz(data)

# Fizzbuzz Class
class Fizzbuzz:
	# Function - Connect to Dynamo.
	def connect(self):
	    session = boto3.Session()
	    dynamodb = session.resource('dynamodb')
	    return dynamodb.Table('fizzbuzz')

	# Function - Search DynamoDB for previous Fizzbuzz ID.
	def getFizzbuzz(self, id):
	    # Connect to Dynamo.
	    fb = self.connect()

	    # Pull entry from Dynamo for that ID.
	    response = fb.get_item(
	        Key={ 'id': id }
	    )
	    if 'Item' in response:
	        return response['Item']
	    else:
	        return 'Sorry, no entry matches that ID.'

	# Function - Process new Fizzbuzz entry from variables given.
	def runFizzbuzz(self, data):
		# Set timestamp.
		currentTime = int(time.time())
		# Create a list of matching numbers.
		matches = []

		# Loop through our number range.
		for z in range(1,(data['z'] + 1)):
			# If divisible by X and Y, add to fizzbuzz.
			if z % data['x'] == 0 and z % data['y'] == 0:
				matches.append(z)

		# Store our data in Dynamo
		response = self.saveFizzbuzz(matches, data, currentTime)
		if 'id' in response:
			# Remove ID and add URL for user.
			response.pop('id')
			response['url'] = 'https://devops-fizzbuzz.mtslzr.io/id/' + str(currentTime)
			return response
		else:
			# Return error response from Dynamo.
			return response

	# Function - Store Fizzbuzz data to DynamoDB.
	def saveFizzbuzz(self, matches, data, currentTime):
		# Connect to Dynamo.
		fb = self.connect()

		# Set up data to be written.
		payload = {
			'id': str(currentTime),
			'x': data['x'],
			'y': data['y'],
			'z': data['z'],
			'fizzbuzzCount': len(matches),
			'matches': matches,
			'timestamp': datetime.fromtimestamp(currentTime).strftime('%Y-%m-%d %H:%M:%S'),
		}

		# Write to database; if successful return True. Otherwise, return the response info.
		response = fb.put_item(Item=payload)
		if response['ResponseMetadata']['HTTPStatusCode'] != 200:
			return payload
		else:
			return response