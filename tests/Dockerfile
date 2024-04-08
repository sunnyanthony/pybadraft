FROM python:3.11

WORKDIR /app

RUN pip install "your-package-name[dev]@ git+https://github.com/sunnyanthony/pybadraft.git"

COPY tests /app/tests

CMD ["pytest", "tests/"]
