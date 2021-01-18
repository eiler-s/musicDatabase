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

class TypecastArtistFn(beam.DoFn):
    def process(self, element):
        a_row = element

        # get the row's attributes to be typecasted
        artist_id = a_row.get('artist_id')
        begin_year = a_row.get('begin_year')
        begin_month = a_row.get('begin_month')
        begin_day = a_row.get('begin_day')
        end_year = a_row.get('end_year')
        end_month = a_row.get('end_month')
        end_day = a_row.get('end_day')
        artist_type = a_row.get('artist_type')
        area_id = a_row.get('area_id')
        gender = a_row.get('gender')
        ended = a_row.get('ended')
        begin_area_id = a_row.get('begin_area_id')
        end_area_id = a_row.get('end_area_id')

        # typecast the row's attributes to correct type or leave as None
        a_row['artist_id'] = int(artist_id) if artist_id else None
        a_row['begin_year'] = int(begin_year) if begin_year else None
        a_row['begin_month'] = int(begin_month) if begin_month else None
        a_row['begin_day'] = int(begin_day) if begin_day else None
        a_row['end_year'] = int(end_year) if end_year else None
        a_row['end_month'] = int(end_month) if end_month else None
        a_row['end_day'] = int(end_day) if end_day else None
        a_row['artist_type'] = int(artist_type) if artist_type else None
        a_row['area_id'] = int(area_id) if area_id else None
        a_row['gender'] = int(gender) if gender else None
        a_row['ended'] = bool(ended) if ended else None
        a_row['begin_area_id'] = int(begin_area_id) if begin_area_id else None
        a_row['end_area_id'] = int(end_area_id) if end_area_id else None
        
        # return data as a tuple
        return [(a_row)]

def run():
    PROJECT_ID = 'earnest-keep-266820'
    BUCKET = 'gs://jeffersonballers-yeet'
    DIR_PATH = BUCKET + '/output/'

    # run pipeline on Dataflow 
    options = {
        'runner': 'DirectRunner',
        'job_name': 'artist-beam',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging'
    }

    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    p = beam.Pipeline('DirectRunner', options=opts)
    
    # query data from big query dataset
    sql = 'SELECT * from musicbrainz_modeled.Artist limit 500'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    # write source PCollection to input file
    query_results | 'Write input.tx' >> WriteToText('input.txt')
    
    # apply ParDo to ReplaceValsFn to change '\N' to nul
    replaced_vals_pcoll = query_results | 'Replace t/f and n' >> beam.ParDo(ReplaceValsFn())

    # write PCollection to log file
    replaced_vals_pcoll | 'Write log 1' >> WriteToText('replaced_vals_pcoll.txt')
    
    # apply ParDo to format the student's date of birth  
    casted_vals_pcoll = replaced_vals_pcoll | 'Typecasts values to correct datatypes.' >> beam.ParDo(TypecastArtistFn())
    
    # write final PCollection to output file
    casted_vals_pcoll | 'Write log 2' >> WriteToText('output.txt')
    
    # create a new data table in the modeled dataset in big query
    dataset_id = 'musicbrainz_modeled'
    table_id = 'Artist_Beam'
    schema_id = 'artist_id:INT64,artist_name:STRING,sort_name:STRING,begin_year:INT64,begin_month:INT64,begin_day:INT64,end_year:INT64,end_month:INT64,end_day:INT64,artist_type:INT64,area_id:INT64,gender:INT64,comment:STRING,ended:BOOL,begin_area_id:INT64,end_area_id:INT64'


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
