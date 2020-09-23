import sys
from google.cloud import storage
import googleapiclient.discovery
import subprocess
import json
import warnings
from urllib.error import HTTPError
from datetime import datetime


def main(name):
    dict_test = {"status": "", "message": "", "projects": []}
    update_dict = {"projectId": "", "status": "", "timestamp": "", "bucket_name": ""}
    current_datetime = str(datetime.now())
    cloudbilling = googleapiclient.discovery.build('cloudbilling', 'v1')
    try:
        cloudbilling_response = cloudbilling.billingAccounts().list().execute()
        billing_account_id = cloudbilling_response['billingAccounts'][0]['name']
        dict_test.update(status="success", message="Setup Completed Successfully")
    except Exception as e:
        billing_account_id=None
        dict_test.update(status="failed", message="Not Authorized to Access Billing Account-Error{}".format(e))
        print(dict_test)
        # print("Error Fetching Billing Account ID Details:", dict_test)
    list1=[]
    if billing_account_id is not None:
        projects_request = cloudbilling.billingAccounts().projects().list(name=billing_account_id)
        try:
            response = projects_request.execute()
            project_output_json = response["projectBillingInfo"]
        except Exception as e:
            dict_test.update(status="failed", message=e)
            # print("Error Fetching Projects Under Billing Account ", dict_test)

        bucket_project_id = subprocess.check_output(
            ["curl", "-s", "http://metadata.google.internal/computeMetadata/v1/project/project-id", "-H",
             "Metadata-Flavor: Google"], shell=False)
        #list1 = []
        for project_row in project_output_json:
            PROJECT_ID = project_row["projectId"]
            # update_dict = {"projectId": PROJECT_ID, "status": "", "timestamp": "", "bucket_name": ""}
            # dict_test['projects']=update_dict
            compute = googleapiclient.discovery.build('compute', 'v1')
            req = compute.projects().get(project=PROJECT_ID)
            try:
                response_validate = req.execute()
                storage_client = storage.Client(project=bucket_project_id)
                bucket_name = 'https://storage.googleapis.com/' + name
                body = {
                    "bucketName": bucket_name,
                }
                try:
                    if (response_validate['usageExportLocation'].get('bucketName') != ""):
                        already_usage_report_gcs_location = str(
                            response_validate['usageExportLocation'].get('bucketName'))

                        update_dict.update(projectId=PROJECT_ID, timestamp=current_datetime, status="success",
                                           bucket_name=already_usage_report_gcs_location)

                        if (name != already_usage_report_gcs_location):
                            GCS_BUCKET = storage_client.get_bucket(name)
                            request = compute.projects().setUsageExportBucket(project=PROJECT_ID, body=body)
                            request.execute()
                            print_message = "We Updated the Usage reports in {} to push Usage reports to {} instead of {}".format(
                                PROJECT_ID, name, already_usage_report_gcs_location)
                            update_dict.update(projectId=PROJECT_ID, timestamp=current_datetime, status="success",
                                               message=print_message,
                                               bucket_name=name)

                    else:
                        GCS_BUCKET = storage_client.get_bucket(name)
                        request = compute.projects().setUsageExportBucket(project=PROJECT_ID, body=body)
                        request.execute()
                        update_dict.update(projectId=PROJECT_ID, timestamp=current_datetime, status="success",
                                           bucket_name=name)
                except Exception as e:
                    update_dict.update(status="failed", timestamp=current_datetime, bucket_name=name, message=e)
            except Exception as e:
                # print("Compute Engine API:Disabled")
                update_dict.update(status="failed", timestamp=current_datetime, message=e)
            list1.append(update_dict)
    else:
        update_dict.update(status="failed", timestamp=current_datetime,
                           message="Service account not authorised to fetch Billing account details")

    dict_test['projects'] = list1
    #print(dict_test)
    return dict_test


if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    main("usage-report-test-bucket-v1234")
