# Vietnam stock data analyzer
## Objectives
The goal of this project is to collect and visualize the stock price of all tickers in Vietnam. There is quite limited access to API for a single business user, this project aim at scrap data from website, clean, extract and load into datawarehouse. The final product is a maintainable/reliable data pipeline with exposed analytic dashboard hosted on cloud, and end authorized users can access to 24/7 with daily updated data.

Data are collected for almost 10 years from 2010 to support the analytic (such as backtest or trading strategy for a particular ticker) steps afterward. For the scope of this project, the stock price will be visualized in time series domain for the monitor pupose only.

The report is live from HERE:
[Link To Report Dashboard](http://ec2-54-255-151-44.ap-southeast-1.compute.amazonaws.com:9999/r/15)

<p align="center">
<image src="./images/price_trend.png" width="80%">
</p>


## Proposed Architecture
To prove the working concept with reasonable cost, below is the proposed architecture solution.

<p align="center">
<image src="./images/architecture.png" width="80%">
</p>

The whole picture derives from 4 main components:
* Data collection: 
This project can be accomplished from 2 data sources: ticker URL & price URL hosted by VNDirect. This is where all Vietnam tickers officially registered.  

    > Ticker URL: https://www.vndirect.com.vn/portal/thong-tin-co-phieu/nhap-ma-chung-khoan.shtml?request_locale=en_GB

    > Price URL: https://www.vndirect.com.vn/portal/thong-ke-thi-truong-chung-khoan/lich-su-gia.shtml?request_locale=en


* ETL/Orchestration:
Airflow is chosen to play the orchestration role to run ETL and scheduling jobs. Airflow is dockerized and deployed on Digital Ocean cloud service. Airflow docker is selected to facilitate CI/CD tasks later on.
Refer here: [Airflow Docker Repository](https://github.com/puckel/docker-airflow)

* Data storage:
Up to now, there are 1710 tickers are registered and ~2.4M records stored on Postgres database maintained by Digital Ocean service. Since there are not so many users access at the moment and the data size is not quite significant big, this selection is cost effective while not affect the performance.

* Data Visualization/Analytics:
    Because of time constraint, only visualization is implemented for this project to visualize the colleted data from Postgres database. Future feature will be Data Analytics with Spark by leveraging available data for 10 years to conduct the backtest as an example.

    The superset visualization tool is also dockerized and hosted on EC2 t2.micro which is free up to 1 year service. This option is very optimal since visualization task is not a heavy hardware consumption.Refer here: [Superset Docker Repository](https://github.com/amancevice/docker-superset)

## Data sources
As mentioned before, there are 2 main data sources including ticker and price. The data are in html format which required the ELT job to include some special techniques to extract needed information.

<p align="center">
<image src="./images/ticker.png" width="80%">
</p>

<p align="center">
<image src="./images/price.png" width="80%">
</p>


## Data model
Data model is simply described as below.
<p align="center">
<image src="./images/models.png" width="40%">
</p>

## ETL Job
Although the data model looks simple, the heavy task of ETL job is to extract the needed data from html format. In this project, Selenium and BeautifulSoup libraries are used to automate the process of crawling stock data.

The pipeline includes 2 main steps: ticker ingestion and price ingestion. At price ingestion step, it is devided into different flows by exchange to enable mutithread processing for better performance. Within each thread, the data quality check has been includeded before every insert command.

<p align="center">
<image src="./images/airflow_dag.png" width="60%">
</p>

<p align="center">
<image src="./images/airflow_treeview.png" width="75%">
</p>

## How the project is organized and setup
* Files in repository:
In the main local dag directory (defined by AIRFLOW_HOME), separate project is put inside to make project more scalable. With this file structure design, where there are other stock data source, just simple create another one beside `vn_stock` project.

Within `vn_stock` project, `dag.py` lists the pipeline design and scheduler setting. It calls the ticker and price ingestion via PythonOperator.

```bash
.
└── vn_stock
    ├── dag.py
    ├── README.md
    └── tasks
        ├── config.py
        ├── data
        ├── etl_vndirect_price.py
        ├── etl_vndirect_ticker.py
        ├── requirements.txt
        ├── setup_database.py
        ├── sql_queries.py
        └── utils.py
```

* Build and run docker-airflow with LocalExecutor setup to enable parallelizing task instances locally:
    > `docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .`

    > `docker-compose -f docker-compose-LocalExecutor.yml up -d`

* Setup database for the first run (access from inside airflow container):
    > `docker exec -it -u 0 docker-airflow_webserver_1 bash`

    > `python vn_stock/task/setup_database.py`

## Scenarios concerns and solution
* Data Increased by 100x: move data storage to AWS Redshift which is designed to handling columnar store for better performance.
* The pipeline would be run on a daily basis by 7 am every day: easily setting with Airflow scheduler. The current one is sheduled on 8AM and 8 PM daily.
* The database needed to be accessed by 100+ people: when the user access increase, it is necessary to setup the role and group for particular purpose (view/modify) to enhance the security management and performance effectiveness. Refer here: [Managing PostgreSQL users](https://aws.amazon.com/blogs/database/managing-postgresql-users-and-roles/)
