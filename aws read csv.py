#############################################################################################################
#   Write code in python, preferably using pandas to do the following :-                                    #
#   1. Ingest file from AWS S3                                                                              #
#   2. Find the number of devices for each age, gender, device_type and state_name                          #
#   3. Find number of devices by each age group. The different age groups(grouped by no. of years) are      #
#   0-18, 19-24,25-34,35-44, 45-54, 55-64,65+                                                               #
#                                                                                                           #
#############################################################################################################


import boto3
import pandas as pd
from io import StringIO
from datetime import datetime,date

# QUESTION 1
# Ingest file from AWS S3 
aws_id = 'AKIAQZSOZTNG557YER57'
aws_secret ='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'
client = boto3.client('s3', aws_access_key_id=aws_id,
        aws_secret_access_key=aws_secret)
bucket_name = 'mcsp-hiring-external'
object_key = 'test_input_file.csv'
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))
# Parsing all date formats date_of_birth
def try_parsing_date(text):
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    raise ValueError('no valid date format found')

# Age Calculation
def calculate_age(born):
    born = try_parsing_date(born)
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

# Adding Age column from dob
df['age'] = df['date_of_birth'].apply(calculate_age)
# spilting device_type_us_state into device_type and us_state
df[['device_type','us_state']] = df.device_type_us_state.str.split("_",expand=True) 
# removing column 
del df['device_type_us_state']

# QUETION 2
# Find the number of devices for each age, gender, device_type and state_name
# Total Number of Devices by Ageprint("Total Number of Devices by Age")
number_of_devices_by_age = df['age'].value_counts(sort=False)
print(number_of_devices_by_age)

# Total Number of Devices by Gender
print("Total Number of Devices by Gender")
number_of_devices_by_gender = df['Gender'].value_counts(sort=False)
print(number_of_devices_by_gender)

# Number of Devices by Device Type
print("Number of Devices by Device Type")
total_devices_by_device_type = df['device_type'].value_counts(sort=False)
print(total_devices_by_device_type)

# Total Number of Devices by State Name
print("Total Number of Devices by State Name")
number_of_devices_by_state = df['us_state'].value_counts(sort=False)
print(number_of_devices_by_state)


# QUESTION 3
# Find number of devices by each age group
# Total Number of Devices by Age Group
print()
labels = ['0-18','19-24','25-34','35-44', '45-54', '55-64','65+']
bins=[0,18,24,34,44,54,64,65]
df['AgeGroup'] = pd.cut(x=df['age'],bins=bins, labels=labels)
number_of_devices_by_age_group = df['AgeGroup'].value_counts(sort=False)
print(number_of_devices_by_age_group)
number_of_devices_by_age_group.to_csv('df.csv')
