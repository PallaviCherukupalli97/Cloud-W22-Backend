from flask import Flask, request, Response 
import boto3
from flask_cors import CORS

app = Flask(__name__)
CORS(app, support_credentials=True)

@app.route('/upload', methods=['POST'])
def upload():
    try:
        storageSession = boto3.Session(
        aws_access_key_id='****************ZEXT',
        aws_secret_access_key='k*************cokILTL1FUYAbMYH'
        )
        s3 = storageSession.resource('s3')
        print(request.data)
        s3.Bucket('wintercloudstack-us-east-1-132895714687').put_object(Key='winterCloud.pdf', Body=request.files["data"])
        return Response(status=200)
    except:
        return Response("Error occurred while posting to AWS S3", status=500)

@app.route('/getFile', methods=["GET"])
def get_file():
    try:
        client = boto3.client('s3', aws_access_key_id='****************ZEXT', aws_secret_access_key='k*************cokILTL1FUYAbMYH')
        csv_obj = client.get_object(Bucket='wintercloudstack-us-east-1-132895714687', Key='winter_cloud_output.csv')
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        return Response(csv_string, status=200)
    except:
        return Response("Error occurrred while getting file details", status=500)
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)