# Lambda-Image-Jupyter

The repository speeds-up configuration needed to start working with **Lambda + Jupyter + VS**

## How to work with repo using devcontainer.json

- To test lambda function locally one can use .devcontainer.json to create and run Docker image locally. 
- Devcontainer installs **ipykernel** what enables Jupyter notebook to be used inside of container. However one should be aware that **ipykernel** would not be installed into docker image uploaded to ECR, what can cause in some situtions packages instalation issues.
- **mounts** - path mounted should point to folder with aws credentials and configuration files

## Lambda Function Code - lambda_container_image

- **Dockerfile** - docker image definition used by Lambda Function
- **Test_lambda_function_code.ipynb** - Jupyter notebook that can be used to test lambda function. Notebook should be used to overwrite **app.py** file
- **app.py** - file with lambda function execution code
- **requirements-lambda.txt** - packages that have to be installed into docker container