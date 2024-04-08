FROM python:3.11

WORKDIR /app

ARG REPO_URL
RUN pip install git+${REPO_URL}#egg=pybadraft[dev]

COPY tests /app/tests

CMD ["pytest", "tests/"]
