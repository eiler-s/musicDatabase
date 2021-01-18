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
        rel_tuple = (element)
        return [rel_tuple]

class TypecastReleaseFn(beam.DoFn):
    def process(self, element):
        rel_row = element

        # get the row's attributes to be typecasted
        rel_id = rel_row.get('rel_id')
        artist_id = rel_row.get('artist_id')
        rel_group = rel_row.get('rel_group')
        status = rel_row.get('status')
        packaging = rel_row.get('packaging')
        language = rel_row.get('language')
        script = rel_row.get('script')
        barcode = rel_row.get('barcode')
        quality = rel_row.get('quality')

        # typecast the row's attributes to correct type or leave as None
        rel_row['rel_id'] = int(rel_id) if rel_id else None
        rel_row['artist_id'] = int(artist_id) if artist_id else None
        rel_row['rel_group'] = int(rel_group) if rel_group else None
        rel_row['status'] = int(status) if status else None
        rel_row['packaging'] = int(packaging) if packaging else None
        rel_row['language'] = int(language) if language else None
        rel_row['script'] = int(script) if script else None
        rel_row['barcode'] = int(barcode) if barcode else None
        rel_row['quality'] =  int(quality) if quality else None
        
        # return data as a tuple
        return [(rel_row)]

def run():
    PROJECT_ID = 'earnest-keep-266820'
    BUCKET = 'gs://jeffersonballers-yeet'
    DIR_PATH = BUCKET + '/output/'

    # run pipeline on Dataflow 
    options = {
        'runner': 'DirectRunner',
        'job_name': 'rel-beam',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging'
    }

    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    p = beam.Pipeline('DirectRunner', options=opts)
    
    # query data from big query dataset
    sql = 'SELECT * from musicbrainz_modeled.Release limit 500'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    # write source PCollection to input file
    query_results | 'Write input.txt' >> WriteToText('input.txt')
    
    # apply ParDo to ReplaceValsFn to change '\N' to nul
    replaced_vals_pcoll = query_results | 'Replace t/f and n' >> beam.ParDo(ReplaceValsFn())

    # write PCollection to log file
    replaced_vals_pcoll | 'Write log 1' >> WriteToText('replaced_vals_pcoll_release.txt')
    
    # apply ParDo to format the student's date of birth  
    casted_vals_pcoll = replaced_vals_pcoll | 'Typecasts values to correct datatypes.' >> beam.ParDo(TypecastReleaseFn())
    
    # write final PCollection to output file
    casted_vals_pcoll | 'Write log 2' >> WriteToText('output.txt')
    
    # create a new data table in the modeled dataset in big query
    dataset_id = 'musicbrainz_modeled'
    table_id = 'Release_Beam'
    schema_id = 'rel_id:INT64,rel_name:STRING,artist_id:INT64,rel_group:INT64,status:INT64,packaging:INT64,language:INT64,script:INT64,barcode:INT64,comment:STRING,quality:INT64'

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
