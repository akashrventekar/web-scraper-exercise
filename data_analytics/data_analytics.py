import boto3
import pandas as pd
import json
from io import StringIO

s3_client = boto3.client('s3')

bucket = 'big-data-set'
file1 = "pr.data.0.Current"
file2 = "population_data.json"

# Part 3.0
pr_data_content = s3_client.get_object(
    Bucket=bucket,
    Key=file1
)['Body'].read().decode()

population_data_json = s3_client.get_object(
    Bucket=bucket,
    Key=file2
)['Body'].read().decode()

population_data_json = json.loads(population_data_json)

population_data_json_df = pd.DataFrame.from_dict(population_data_json['data'], )
pr_data_content_df = pd.read_csv(StringIO(pr_data_content), delim_whitespace=True)

# Part 3.1
print(population_data_json_df['Population'].std())
print(population_data_json_df['Population'].mean())

# Part 3.2
df2 = pr_data_content_df.loc[:, ['series_id', 'year', 'period', 'value']].groupby(
    ['series_id', 'year']).sum().reset_index()

res = df2[df2['value'] == df2.groupby(['series_id'])['value'].transform('max')]
print(res)

# Part 3.3

join_result = pd.merge(population_data_json_df.loc[: , ['Population', 'ID Year']], pr_data_content_df.loc[(pr_data_content_df['period'] == 'Q01') & (pr_data_content_df['series_id'] == 'PRS30006032'), ['series_id', 'year', 'period', 'value']], left_on='ID Year', right_on='year').loc[:, ['series_id','year', 'period', 'value', 'Population']]
print(join_result)
