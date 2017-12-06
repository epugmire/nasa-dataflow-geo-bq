import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import json
import urllib2
import re
import logging
import argparse
# As part of the initial setup, install Google Cloud Platform specific extra components.
#pip install apache-beam[gcp]
#python -m apache_beam.examples.wordcount_minimal --input gs://dataflow-samples/shakespeare/kinglear.txt \
#                                                 --output gs://<your-gcs-bucket>/counts \
#                                                 --runner DataflowRunner \
#                                                 --project your-gcp-project \
#                                                 --temp_location gs://<your-gcs-bucket>/tmp/

p = beam.Pipeline(options=PipelineOptions())
##--<option>=<value>

def run(argv=None):
	"""Main entry point; defines and runs the wordcount pipeline."""

	parser = argparse.ArgumentParser()
	parser.add_argument('--input',
					dest='input',
                    help='Input for the pipeline',
                    default='gs://nasa-dataflow-geo-bq/oneline.csv')
	parser.add_argument('--output',
					dest='output',
                    help='Output for the pipeline',
                    default='gs://nasa-dataflow-geo-bq/results')

	known_args, pipeline_args = parser.parse_known_args(argv)

	pipeline_args.extend([
	  '--runner=DirectRunner',
	  '--project=nasa-dataflow-geo-bq',
	  '--staging_location=gs://nasa-dataflow-geo-bq/staging',
	  '--temp_location=gs://nasa-dataflow-geo-bq/temp',
	  '--job_name=nasa-dataflow-geo-bq',
	])

	pipeline_options = PipelineOptions(pipeline_args)
	pipeline_options.view_as(SetupOptions).save_main_session = True

	with beam.Pipeline(options=pipeline_options) as p:
		lines = p | 'ReadMyFile' >> beam.io.ReadFromText(known_args.input)

		class getURL(beam.DoFn):
			def process(self, element):
				lat_long = re.findall(r'\"(.+?)\"',element)
				if lat_long:
					url_string = 'https://maps.googleapis.com/maps/api/geocode/json?latlng=' + lat_long[0] + '&location_type=APPROXIMATE&result_type=country|locality&key=AIzaSyCc_Z2Jqqa1JLhZhSaURtUhF-N34PCQwrw'
					response = urllib2.urlopen(url_string)
					response_dict = json.load(response)
					if response_dict['status'] == 'ZERO_RESULTS':
						formatted_address = response_dict['status']
					else:
						formatted_address = response_dict['results'][0]['formatted_address']
					return [lat_long[0] + ','+element.rsplit(',',1)[1] + ',' + url_string]
				##return ['https://maps.googleapis.com/maps/api/geocode/json?latlng=' + lat_long[0] + '&location_type=APPROXIMATE&result_type=country|locality&key=AIzaSyCc_Z2Jqqa1JLhZhSaURtUhF-N34PCQwrw']

		##urlData = "http://api.openweathermap.org/data/2.5/weather?q=Boras,SE"

		URLs = lines | beam.ParDo(getURL())

# Flatten takes a tuple of PCollection objects.
# Returns a single PCollection that contains all of the elements in the PCollection objects in that tuple.
		merged = (
		    (lines,URLs)
		    # A list of tuples can be "piped" directly into a Flatten transform.
		    | beam.Flatten())

		merged | beam.io.WriteToText(known_args.output)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
