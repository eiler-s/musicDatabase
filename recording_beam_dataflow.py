import datetime
import apache_beam as beam
from apache_beam.io import WriteToText
import logging

# This class is to replace problematic values in our data with values that are appropriate for casting.
# We were unable to cast the values in SQL because the value used for 'null' in our data was a string ('\N')
# This function changes \N values to None and the literal strings 't' and 'f' to True/False strings so that
# The columns are prepared for casting to the appropriate data types.
class ReplaceValsFn(beam.DoFn):
    def process(self, element):
        # Each element is one record in our data
        record = element
        for key in record:
            # if the column has \N change to None
            if record[key] == r'\N':
                record[key] = None
            # if the column has 't' change to 'True'
            elif record[key] == 't':
                record[key] = 'True'
            # if the column has 'f' change to 'False'
            elif record[key] == 'f':
                record[key] = 'False'
        # return data as a tuple
        rec_tuple = (element)
        return [rec_tuple]

class TypecastrecFn(beam.DoFn):
    def process(self, element):
        rec_row = element

        # get the row's attributes to be typecasted
        rec_id = rec_row.get('rec_id')
        artist_id = rec_row.get('artist_id')
        length = rec_row.get('length')

        # typecast the row's attributes to correct type or leave as None
        rec_row['rec_id'] = int(rec_id) if rec_id else None
        rec_row['artist_id'] = int(artist_id) if artist_id else None
        rec_row['length'] = int(length) if length else None
        
        # return data as a tuple
        return [rec_row]

def run():
    PROJECT_ID = 'earnest-keep-266820'
    BUCKET = 'gs://jeffersonballers-yeet'
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # run pipeline on Dataflow 
    options = {
        'runner': 'DataflowRunner',
        'job_name': 'recording-beam-dataflow',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
        'num_workers': 1
    }

    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    p = beam.Pipeline('DataflowRunner', options=opts)
    
    # query data from big query dataset
    sql = 'SELECT * from musicbrainz_modeled.Recording'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    # write source PCollection to input file
    query_results | 'Write input.tx' >> WriteToText(DIR_PATH + 'input.txt')
    
    # apply ParDo to ReplaceValsFn to change '\N' to nul
    replaced_vals_pcoll = query_results | 'Replace t/f and n' >> beam.ParDo(ReplaceValsFn())

    # write PCollection to log file
    replaced_vals_pcoll | 'Write log 1' >> WriteToText(DIR_PATH + 'replaced_vals_pcoll.txt')
    
    # apply ParDo to format the student's date of birth  
    casted_vals_pcoll = replaced_vals_pcoll | 'Typecasts values to correct datatypes.' >> beam.ParDo(TypecastrecFn())
    
    # write final PCollection to output file
    casted_vals_pcoll | 'Write log 2' >> WriteToText(DIR_PATH + 'output.txt')
    
    # create a new data table in the modeled dataset in big query
    dataset_id = 'musicbrainz_modeled'
    table_id = 'Recording_Beam_DF'
    schema_id = 'rec_id:INT64,rec_name:STRING,artist_id:INT64,length:INT64,comment:STRING'

    # write final PCollection to new BQ table
    casted_vals_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                                      table=table_id, 
                                                                      schema=schema_id,
                                                                      project=PROJECT_ID,
                                                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                                      batch_size=int(100))
    
    result = p.run()
    result.wait_until_finish()      

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()