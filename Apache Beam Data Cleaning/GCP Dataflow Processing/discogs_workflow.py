import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    'start_date': datetime.datetime(2020, 5, 3)
}

# big query project and dataset names
project = '`earnest-keep-266820`'
staging_dataset = 'discogs_workflow_staging'
modeled_dataset = 'discogs_workflow_modeled'

# SQL and command line commands
query_cmd = 'bq query --use_legacy_sql=false '

def load_cmd (dataset, table, file_loc):
    cmd_str = ('bq --location=US load --autodetect --max_bad_records 10000 --source_format=NEWLINE_DELIMITED_JSON ' +
                dataset + "." + table + ' ' + file_loc)
    return cmd_str

def table_cmd (new_set, new_table, sel_str, old_set, old_table, where_str = None):
    cmd_str = ('create or replace table ' + new_set + '.' + new_table + ' ' +
                'as select ' + sel_str + ' ' +
                'from ' + old_set + "." + old_table + ' ')
    if where_str is not None:
        cmd_str += 'where ' + where_str
    return cmd_str


# SQL clauses
is_bad_or_null = 'data_quality.val <> \"Entirely Incorrect\" or data_quality.val is null'

modeled_artist_url_schema = ('GENERATE_UUID() uuid, ' + 
                             'z.Zid as group_id, ' +
                             'a.id.Val as artist_id, ' + 
                             'y.Val as url')

modeled_band_schema = ('n.val as name, ' + 
                       'n.Zid as id')

clean_band_schema = ('distinct *')

modeled_artist_schema = ('GENERATE_UUID() uuid, ' + 
                         'id.val as id, ' + 
                         'x.Zid as band_id, ' +
                         'a.realname.val as name, ' +
                         'a.profile.val as profile')

modeled_aliases_schema = ('GENERATE_UUID() uuid, ' +
                          'id.val as artist_id, ' + 
                          'x.Zid as band_id, ' +
                          'a.name.Val as name, ' +
                          'y.Zid as alias_id, ' +
                          'y.val as alias')

modeled_variations_schema = ('GENERATE_UUID() uuid, ' +
                             'id.val as artist_id, ' +
                             'x.Zid as band_id, ' +
                             'y.val as variation')

modeled_label_schema = ('id.Val as label_id, ' +
                        'contactinfo.Val as label_contact, ' +
                        'parentLabel.Val as par_name, ' +
                        'parentLabel.Zid as par_id, ' +
                        'name.Val as label_name, ' +
                        'profile.Val as label_profile')

modeled_label_url_schema = ('GENERATE_UUID() as uuid, ' +
                            'id.Val as label_id, ' +
                            'lu.Val as url')

bad_par_self_rjoin = 'Label l1 right join '+modeled_dataset+'.Label l2 on l1.label_id = l2.par_id'
is_no_par_label = 'l1.label_id is null and l2.par_name is not null'

new_label_id_parts = ('par_name, ' +
                    'par_id as old_id, ' + 
                    '(select max(L.label_id) from '+modeled_dataset+'.Label L) as max_id, ' +
                    'row_number() over() as row_num')
new_label_ids = 'par_name, old_id, (max_id + row_num) as new_id'

union_new_labels = ('union all ' +
                    'select ' +
                        'new_id as label_id, ' +
                        'null as contact, ' +
                        'null as par_name, ' +
                        'null as par_id, ' +
                        'par_name as label_name, ' +
                        'null as label_profile ' +
                    'from '+modeled_dataset+'.Label_SQL_3')

with models.DAG(
        'discogs_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    
# DATASET CREATION TASKS
    # create the discogs dataset in big query
    create_staging_dataset = BashOperator(
            task_id='create_staging_dataset',
            bash_command= 'bq --location=US mk --dataset ' + staging_dataset)
    
    # create the discogs dataset in big query
    create_modeled_dataset = BashOperator(
            task_id='create_modeled_dataset',
            bash_command= 'bq --location=US mk --dataset ' + modeled_dataset)
    
# STAGING TABLE CREATION AND CLEANING TASKS
    # create the artist table in big query
    create_staging_artist = BashOperator(
            task_id='create_staging_artist',
            bash_command= load_cmd (staging_dataset, 'Artist', 'gs://cs327e_discogs/final_artist.json'),
            trigger_rule='all_done')
    
    # create the label table in big query
    create_staging_label = BashOperator(
            task_id='create_staging_label',
            bash_command= load_cmd (staging_dataset, 'Label', 'gs://cs327e_discogs/labels_final.json'),
            trigger_rule='all_done')
    
    # clean artist table by removing nulls and bad data
    clean_staging_artist = BashOperator(
            task_id='clean_staging_artist',
            bash_command= (query_cmd + "'" +
                           table_cmd (staging_dataset, 'Artist_Clean', '*',
                                      staging_dataset, 'Artist', is_bad_or_null) +
                           "'"), 
            trigger_rule='all_done')
    
    # clean label table by removing nulls and bad data
    clean_staging_label = BashOperator(
            task_id='clean_staging_label',
            bash_command= (query_cmd + "'" +
                           table_cmd (staging_dataset, 'Label_Clean', '*',
                                      staging_dataset, 'Label', is_bad_or_null) +
                           "'"), 
            trigger_rule='all_done')

# MODELED TABLE CREATION TASKS
    # artistURL - Nicole
    create_modeled_artist_url = BashOperator(
        task_id='create_modeled_artist_url',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Artist_URL', modeled_artist_url_schema,
                                  staging_dataset, 'Artist_Clean a, a.groups.name z, a.urls.url y', None) +
                       "'"), 
        trigger_rule='all_done')
    
    # Band - Nicole
    create_modeled_band = BashOperator(
        task_id='create_modeled_band',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Band', modeled_band_schema,
                                  staging_dataset, 'Artist_Clean a, a.groups.name as n', None) +
                       "'"), 
        trigger_rule='all_done')
    
    # Clean Band - Nicole
    clean_modeled_band = BashOperator(
        task_id='clean_modeled_band',
        bash_command= (query_cmd + "'" +
                       'create or replace table '+modeled_dataset+'.Band '+
                       'as select distinct * from '+modeled_dataset+'.Band'+
                       "'"), 
        trigger_rule='all_done')
    
    # Artist - Nicole
    create_modeled_artist = BashOperator(
        task_id='create_modeled_artist',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Artist', modeled_artist_schema,
                                  staging_dataset, 'Artist_Clean a, a.groups.name x', None) +
                       "'"), 
        trigger_rule='all_done')
    
    # Aliases - Nicole
    create_modeled_aliases = BashOperator(
        task_id='create_modeled_aliases',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Aliases', modeled_aliases_schema,
                                  staging_dataset, 'Artist_Clean a, a.groups.name x, a.aliases.name y', None) +
                       "'"), 
        trigger_rule='all_done')
    
    # variations - Nicole
    create_modeled_variations = BashOperator(
        task_id='create_modeled_variations',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Variations', modeled_variations_schema,
                                  staging_dataset, 'Artist_Clean a, a.groups.name x, a.namevariations.name y', None)+
                       "'"), 
        trigger_rule='all_done')
    
    # create a table for the modeled labels
    create_modeled_label = BashOperator(
        task_id='create_modeled_label',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Label', modeled_label_schema,
                                  staging_dataset, 'Label_Clean', None) +
                       "'"), 
        trigger_rule='all_done')
    
    # create a table for the modeled label URLs
    create_modeled_label_url = BashOperator(
        task_id='create_modeled_label_url',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Label_URL', modeled_label_url_schema,
                                  staging_dataset, 'Label_Clean as l, l.urls.url as lu', None) +
                       "'"), 
        trigger_rule='all_done')
    
# MODELED TABLE TRANSFORMATION TASKS
    # create table identifying parent labels that do not have their own records
    create_sql_1 = BashOperator(
        task_id='create_sql_1',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Label_SQL_1', 'distinct l2.par_id, l2.par_name',
                                  modeled_dataset, bad_par_self_rjoin, is_no_par_label) +
                       "'"), 
        trigger_rule='all_done')
    
    # create table pairing bad parent labels with metadata needed for their new label ids
    create_sql_2 = BashOperator(
        task_id='create_sql_2',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Label_SQL_2', new_label_id_parts,
                                  modeled_dataset, 'Label_SQL_1', None) +
                       "'"), 
        trigger_rule='all_done')
    
    # create table containing bad parent labels with new label ids
    create_sql_3 = BashOperator(
        task_id='create_sql_3',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Label_SQL_3', new_label_ids,
                                  modeled_dataset, 'Label_SQL_2', None) +
                       "'"), 
        trigger_rule='all_done')
    
    # create table that unions the existing label table with the new labels generated in previous task
    create_sql_final = BashOperator(
        task_id='create_sql_final',
        bash_command= (query_cmd + "'" +
                       table_cmd (modeled_dataset, 'Label_SQL_Final', '*',
                                  modeled_dataset, 'Label ' + union_new_labels, None) +
                       "'"), 
        trigger_rule='all_done')
    
# DAG BRANCHING TASKS
    # branch to stage and clean the label and artist tables in parallel
    branch_staging = DummyOperator(
            task_id='branch_staging',
            trigger_rule='all_done')
    
    # branch to model the artist tables in parallel
    branch_artist_modeling = DummyOperator(
            task_id='branch_artist_modeling',
            trigger_rule='all_done')
    
    # branch to model the label tables in parallel
    branch_label_modeling = DummyOperator(
            task_id='branch_label_modeling',
            trigger_rule='all_done')
    
# ARRANGE THE NODES OF THE DAG
    # create the label and artist tables in parallel after creating the datasets
    create_staging_dataset >> create_modeled_dataset >> branch_staging
    
    # model the entities from the artist table independently after cleaning the created table
    branch_staging >> create_staging_artist >> clean_staging_artist >> branch_artist_modeling
    branch_artist_modeling >> create_modeled_artist_url
    branch_artist_modeling >> create_modeled_band >> clean_modeled_band
    branch_artist_modeling >> create_modeled_artist
    branch_artist_modeling >> create_modeled_aliases
    branch_artist_modeling >> create_modeled_variations
    
    # model the label tables independently after cleaning the created table, perform SQL transformation
    branch_staging >> create_staging_label >> clean_staging_label >> branch_label_modeling
    
    branch_label_modeling >> create_modeled_label_url
    branch_label_modeling >> create_modeled_label >> [create_sql_1, create_sql_2, create_sql_3, create_sql_final]