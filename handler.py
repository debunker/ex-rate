import pandas as pd
import requests
import boto3
from io import StringIO

DESTINATION = 'dataocean-etl-test/amplitude' # already created on S3
payload = {}
headers = {
  'Authorization': 'Basic NTk2NWUzZDQzNjY0MDIxMDk5NDA4ODdhZjVmMDZlN2I6NDY0MjYzOGNhYzIyZDM3NGVkY2E4NTMwNDhmNjk5NzE='
}

#udf write to s3
def _write_dataframe_to_csv_on_s3(dataframe, filename):
    """ Write a dataframe to a CSV on S3 """
    print("Writing {} records to {}".format(len(dataframe), filename))
     # Create buffer
    csv_buffer = StringIO()
    # Write dataframe to buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # Create S3 object
    s3_resource = boto3.resource("s3")
    # Write buffer to S3 object
    s3_resource.Object(DESTINATION, filename).put(Body=csv_buffer.getvalue())
    return

#Get active-users
def get_active_users(payload,headers):
    #call the endpoint
    url = 'https://amplitude.com/api/3/chart/9yh65sn/query'
    response = requests.request("GET", url, headers=headers, data = payload)
    #parse json to df
    data = response.json()
    date = data["data"]["xValues"]
    value = [x['value'] for x in data["data"]["series"][0]]
    active_users=pd.DataFrame({'date':date,'value':value})
    #write to s3
    _write_dataframe_to_csv_on_s3(active_users,'amplitude_1.csv',index=False)
    return

def users_by_company_30days(payload,headers):
    #call the endpoint
    url = 'https://amplitude.com/api/3/chart/9yh65sn/query'
    response = requests.request("GET", url, headers=headers, data = payload)
    #parse json to df
    data = response.json()
    company = data["data"]["xValues"]
    value = data["data"]["series"][0]
    users_by_company_30days=pd.DataFrame({'company':company,'value':value})
    #write to s3
    _write_dataframe_to_csv_on_s3(users_by_company_30days,'users_by_company_30days.csv',index=False)
    return
  
def view_create_case(payload,headers):
    #call the endpoint
    url = 'https://amplitude.com/api/3/chart/2zcd867/query'
    response = requests.request("GET", url, headers=headers, data = payload)
    #parse json to df
    data = response.json()
    b = [x[0]['value'] for x in data["data"]["seriesCollapsed"]]
    a = ['Create' if x['eventIndex']==1 else 'View' for x in data["data"]["seriesMeta"]]
    x = data["data"]["seriesLabels"]
    df2=pd.DataFrame({'company':x,'event_index':a,'data':b})
    #write to s3
    _write_dataframe_to_csv_on_s3(df2,'view_create_case.csv',index=False)
    return

def endpoint(_event, _context):
    
    print("Loading into S3")
    #run etl
    view_create_case(payload,headers)
    users_by_company_30days(payload,headers)
    get_active_users(payload,headers)
    return {'statusCode': 200, 'body': "Done."}


""" def endpoint(event, context):
    current_time = datetime.datetime.now().time()
    body = {
        "message": "Hello, the current time is " + str(current_time)
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response
 """