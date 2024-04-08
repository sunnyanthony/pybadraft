FROM python:3.11

WORKDIR /app

ARG REPO_URL
RUN pip install "pybadraft[dev] @ git+${REPO_URL}"

COPY tests /app/tests

CMD ["pytest", "tests/"]
