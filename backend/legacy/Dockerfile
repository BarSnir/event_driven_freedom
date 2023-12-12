FROM python:3.6
EXPOSE 5000
WORKDIR /app
COPY requirements.txt /app
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org -r requirements.txt
COPY app.py /app
CMD python app.py

# WORKDIR /legacy_server
# COPY requirements.txt /code
# RUN --mount=type=cache,target=/root/.cache/pip \
#     pip3 install -r requirements.txt

# COPY . .

# ENV FLASK_APP main.py
# ENV FLASK_ENV development
# ENV FLASK_RUN_PORT 8000
# ENV FLASK_RUN_HOST 0.0.0.0

# EXPOSE 8091

# CMD ["flask", "run"]

# FROM builder AS dev-envs

# RUN <<EOF
# apk update
# apk add git
# EOF

# RUN <<EOF
# addgroup -S docker
# adduser -S --shell /bin/bash --ingroup docker vscode
# EOF

# # install Docker tools (cli, buildx, compose)
# COPY --from=gloursdocker/docker / /

# CMD ["flask", "run"]