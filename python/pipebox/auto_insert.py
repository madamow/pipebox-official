from pipebox import pipequery

# Create db connection
pipeline = pipequery.PipeQuery('db-desoper')

# Get operations propids and programs for query 
propid,program = pipeline.get_propids_programs()

# Insert exposures into mjohns44.auto_queue
pipeline.insert_auto_queue(nites=['20171001'],program=program,propid=propid)

