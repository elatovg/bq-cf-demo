# import base64
# import json
import datetime
import os
from google.cloud import bigquery
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def process_storage_event(data, context):
    # data_buffer = base64.b64decode(data['data'])
    # event_entry = json.loads(data_buffer)['protoPayload']

    # Troubleshooting Storage Events
    print(data)
    print(context)

    # bucket = json.loads(data_buffer)['resource']['labels']['bucket_name']
    # bucket_file = json.loads(data_buffer)['resource']['labels']['bucket_name']
    bucket = data['bucket']
    bucket_file = data['name']

    print("Triggered Bucket: {}".format(bucket))
    print("Triggered File in Bucket: {}".format(bucket_file))

    query_bq(bucket_file)

def query_bq(gs_csv_file):
    file_basefile = gs_csv_file.split('.')[-2]
    print(file_basefile)
    current_date  = datetime.datetime.strptime(file_basefile, '%m-%d-%Y')
    # Construct a BigQuery client object.
    client = bigquery.Client()

    query = """
        SELECT SUM(Confirmed) AS SUM_CONFIRMED, 
        SUM(Deaths) AS SUM_DEATHS, SUM(Recovered) AS SUM_RECOVERED, 
        SUM(Active) AS SUM_ACTIVE
        FROM covid.march_results
        WHERE DATE(Last_Update) = @current_date
    """

    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("current_date", "DATE", current_date.date()),
    ]
)
    query_job = client.query(query, job_config=job_config, project=os.environ.get('PROJECT_ID'))  # Make an API request.
    # query_job = client.query(query, job_config=job_config, project=project)  # Make an API request.

    print("The query data:")
    # for row in query_job:
    #     print("")
    #     for data in row:
    #         print(data, "\t", end='')

    rows = query_job.result()
    # print without html formatting
    # 
    # format_string = "{!s:<16} " * len(rows.schema)
    # field_names = [field.name for field in rows.schema]
    # print(format_string.format(*field_names))  # Prints column headers.
    # for row in rows:
    #     print(format_string.format(*row))  # Prints row data.

    # format for email
    table = "<table>\n"

    # Create the table's column headers
    header = [field.name for field in rows.schema]
    table += "  <tr>\n"
    for column in header:
        # table += "    <th>{0}</th>\n".format(column.strip())
        table += "    <th>{0}</th>\n".format(column)
    table += "  </tr>\n"

    # Create the table's row data
    for row in rows:
        table += "  <tr>\n"
        for column in row:
            # table += "    <td>{0}</td>\n".format(column.strip())
            table += "    <td>{0}</td>\n".format(column)
        table += "  </tr>\n"

    table += "</table>"

    # print(table)
    subject = "{}-{}-{}".format("Google","COVID",current_date.strftime("%Y%m%d"))
    send_mail(table,subject)

def send_mail(msg_body, msg_subject):

    message = Mail(
        from_email='cf@sink.sendgrid.net',
        # to_emails=recipients,
        to_emails=os.environ.get('RECIPIENT'),
        subject=msg_subject,
        html_content=msg_body)
    try:
        sg_client = SendGridAPIClient(os.environ.get('SG_API_KEY'))
        # sg_client = SendGridAPIClient(sg_api)
        response = sg_client.send(message)
        # Uncomment for extra debug info
        # print(response.status_code)
        # print(response.body)
        # print(response.headers)
        print('Email Successfully sent with status code: {}.'.format(response.status_code))
    except Exception as e:
        print(e.message)

# when launching for testing :)
# if __name__ == '__main__':
#     query_bq("03-31-2020.csv")