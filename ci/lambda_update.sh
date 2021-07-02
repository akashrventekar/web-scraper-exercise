#!/bin/bash

pip3 install -U -r requirements.txt -t ./dist
cp web-scraper/*.py dist/

cd dist/
zip my-test-web-scraper.zip -r ./

aws lambda update-function-code --function-name scrappy_lambda --zip-file fileb://my-test-web-scraper.zip --profile my-aws-account
