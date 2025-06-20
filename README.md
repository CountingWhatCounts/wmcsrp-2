# West Midlands Cultural Sector Research Project 2.0



## Introduction

This repository contains the source code and documentation to carry out data aggregation and processing for the West Midlands Cultural Sector Research Project 2.0.
Openness is a core principal of this project, and therefore all code and access to the data is being published here.

The list of sourcess is in the table below. To ease replication of the data processing, data from the various sources
has been downloaded and stored in a Google Cloud bucket which is automatically downloaded and processed using this project.


## How to use this project

It supports two modes:
- **Postgres mode** (for private team members with DB access)
- **DuckDB mode** (for public users)

For both you will need the name of a Google Cloud storage bucket where the raw data is stored. This is available upon request from marc.dunford@culturecounts.cc.


### Setup Instructions

#### 1. Clone the repository

```bash
git clone https://github.com/your-org/wmcsrp-2.git
cd wmcsrp-2
```

#### 2. Create a virtual environment (optional but recommended)

```bash
python -m venv venv
source venv/bin/activate
```

#### 3. Install dependencies

```bash
pip install -r requirements.txt
```

#### 4. Configure project

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` to specify
* whether to use postgres or duckdb
* the name of the Google Cloud storage bucket (see above)
* the credentials of the postgres database (if you are using)
* the location where you want the raw data and preprocessed data to be saved


### Running the Pipeline

#### Run all steps in the pipeline

```bash
python run.py
```

### Run specific steps

```bash
python run.py --steps download preprocess load dbt
```

Available steps:

- `download` – fetch raw files from GCS bucket
- `preprocess` – convert to cleaned parquet format
- `load` – load into Postgres or DuckDB depending on config
- `dbt` – run dbt transformations
- `clean` – remove local data folders


## Need Help?

For questions, contact: marc.dunford@culturecounts.cc



## Data Sources

The following are the key data sources used in the project

| Data Theme             | Data Source                                 | Link                                                                                                                                       |
| :--------------------- | :------------------------------------------ | :----------------------------------------------------------------------------------------------------------------------------------------- |
| Cultural Participation | Participation Survey                        | https://beta.ukdataservice.ac.uk/datacatalogue/studies/study?id=9351                                                                       |
| Cultural Participation | Indigo/YouGov Residents Survey              |                                                                                                                                            |
| Demographics           | Census 2021                                 | https://www.nomisweb.co.uk/sources/census_2021_bulk                                                                                        |
| Demographics           | Annual Survey Economic Data                 | https://www.nomisweb.co.uk/query/construct/summary.asp?mode=construct&version=0&dataset=17                                                 |
| Demographics           | Annual Personal Wellbeing Estimates         | https://www.ons.gov.uk/peoplepopulationandcommunity/wellbeing/datasets/headlineestimatesofpersonalwellbeing                                |
| Place Characteristics  | Levelling Up and Priority Places            | https://www.artscouncil.org.uk/your-area/priority-places-and-levelling-culture-places#t-in-page-nav-3                                      |
| Place Characteristics  | Indices of Multiple Deprivation             | https://www.nomisweb.co.uk/query/construct/summary.asp?mode=construct&version=0&dataset=17                                                 |
| Place Characteristics  | Rural Urban Classification                  | https://geoportal.statistics.gov.uk/datasets/ons::rural-urban-classification-2011-of-msoas-in-ew/about                                     |
| Area Mapping           | ONS Postcode Directory (November 2024)      | https://geoportal.statistics.gov.uk/datasets/b54177d3d7264cd6ad89e74dd9c1391d/about                                                        |
| Area Mapping           | MSOA to Local Authority District Mapping    | https://geoportal.statistics.gov.uk/datasets/ons::msoa-2011-to-msoa-2021-to-local-authority-district-2022-exact-fit-lookup-for-ew-v2/about |
| Funding                | Arts Council England NPO Funding            | https://www.artscouncil.org.uk/how-we-invest-public-money/2023-26-Investment-Programme/2023-26-investment-programme-data                   |
| Funding                | Arts Council England Project Grants Funding | https://www.artscouncil.org.uk/ProjectGrants/project-grants-data                                                                           |


## Data Model

You can view the resulting data model and documentation on its creation here https://countingwhatcounts.github.io/wmcsrp-2.
