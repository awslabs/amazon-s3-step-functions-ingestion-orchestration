aws cloudformation create-stack --stack-name awsetlstepfunction --template-body file:///<my-location>/CFN/aws-etl-stepfunction.json  --region us-west-2 --capabilities CAPABILITY_IAM  --parameters file:///<my-location>/scripts/CFN/stepfunction-parameters.json