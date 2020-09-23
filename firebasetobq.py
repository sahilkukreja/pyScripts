#Cloud shell script 
import subprocess

collections=['incidentreport','userRoles']
bucketName = "gs://utadata/new/"
DataSetName='txcope'
for item in collections:
    path=bucketName+item+'/'
    firebase2gcs ="gcloud firestore export "+path+" --collection-ids="+item
    output = subprocess.check_output(['bash','-c', firebase2gcs])
    Target_table=DataSetName+'.'+item
    gcs2bg='bq load --source_format=DATASTORE_BACKUP '+Target_table+' '+path+'all_namespaces/kind_'+item+'/all_namespaces_kind_'+item+'.export_metadata'
    output = subprocess.check_output(['bash','-c', gcs2bg])