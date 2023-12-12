# 
FROM python:3.10
 
WORKDIR /code

COPY ./requirements.txt /code/requirements.txt
 
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir --upgrade -r /code/requirements.txt
 
COPY ./api /code/api
 
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "80"]
