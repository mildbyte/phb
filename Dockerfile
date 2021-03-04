FROM python:3.9

RUN pip install poetry==1.1.5
RUN poetry config virtualenvs.create false

RUN mkdir /phb
COPY phb.py poetry.lock pyproject.toml /phb/
WORKDIR /phb
RUN poetry install

CMD ["python3", "/phb/phb.py"] 
