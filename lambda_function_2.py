import os
import json
import boto3
import io
from collections import ChainMap
import boto3
import io
import json
import pandas as pd
from operator import itemgetter

key_list = []
value_list = {}
key_value_list = []
block_type_obj = {}
block_type1 = "WORD"
block_type2 = "SELECTION_ELEMENT"
block_type3 = "KEY_VALUE_SET"
childRelation = "CHILD"
valueRelation = "VALUE"

def lambda_handler(event, context):
    try:
        if "Records" in event:
            processActions(os.environ["BUCKETNAME"], json.loads(event["Records"][0]["Sns"]["Message"])["JobId"])
            return { "statusCode": 200, "body": json.dumps("File uploaded successfully to S3!")}
    except Exception:
        return {"statusCode": 500, "body": json.dumps("Error parsing the response!")}

def processActions(s3_bucket, jobId):
    dataframe = pd.DataFrame()
    textract = boto3.client("textract")
    s3_storage = boto3.client("s3")
    flatfileStore = io.StringIO()
    input_page = []
    response = {}
    response = textract.get_document_analysis(JobId=jobId)
    input_page.append(response)
    pages = []
    for page in input_page:
        pages.extend(page["Blocks"])
    get_blockType_by_word(pages)
    createKeyValues(pages)
    for key in key_list:
        key_value_list.append([key[0],[(value_list[iteration]) for iteration in key[1]]])
    dataframe["Keys"] = list(map(itemgetter(0), key_value_list))
    dataframe["Values"] = list(map(itemgetter(1), key_value_list))
    dataframe.to_csv(flatfileStore)
    s3_storage.put_object(Body=flatfileStore.getvalue(), Bucket=s3_bucket, Key=f"winter_cloud_output.csv")

def get_blockType_by_word(pages):
    for segment in pages:
        if segment["BlockType"] == block_type1:
            block_type_obj[segment["Id"]] = segment["Text"]
        if segment["BlockType"] == block_type2:
            block_type_obj[segment["Id"]] = segment["SelectionStatus"]

def createKeyValues(pages):
    for segment in pages:
        if (segment["BlockType"] == block_type3):
            if "VALUE" in segment["EntityTypes"]:
                if "Relationships" in segment:
                    for relation in segment["Relationships"]:
                        if relation["Type"] == childRelation:
                            value_list[segment["Id"]] = [block_type_obj[iteration] for iteration in relation["Ids"]]
                else:
                    value_list[segment["Id"]] = "No_value_found"
            if "KEY" in segment["EntityTypes"]:
                    for relation in segment["Relationships"]:
                        if relation["Type"] == valueRelation:
                            value_id = relation["Ids"]
                        if relation["Type"] == childRelation:
                            key_list.append([([block_type_obj[iteration] for iteration in relation["Ids"]]), value_id])