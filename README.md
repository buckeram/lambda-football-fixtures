# lambda-football-fixtures

AWS Lambda to download football match fixtures from https://www.football-data.co.uk/fixtures.csv and store the file in S3

```
$ /usr/local/opt/python\@3.7/bin/python3 -m venv .venv
$ source .venv/bin/activate
$ pip install requests
$ cd .venv/lib/python3.7/site-packages/
$ zip -r ../../../../deployment.zip .
$ cd ../../../../
$ zip -g my-deployment-package.zip lambda_function.py
```

Upload the zip to AWS Lambda

Create the S3 bucket

Create the environment variables for the Lambda

Create the CloudWatch (EventBridge) rule to schedule an event to trigger the lambda

