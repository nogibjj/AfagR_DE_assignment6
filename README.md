## Python Script interacting with SQL Database

[![CI](https://github.com/nogibjj/AfagR_DE_assignment5/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/AfagR_DE_assignment5/actions/workflows/cicd.yml)


## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Usage](#usage)
- [Queries Explanation](#queries-explanation)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project is focused on managing and querying datasets in a cloud-based data environment(Databricks) using SQL and Python. It automates data extraction and transformation, and provides streamlined ways to load the data into a database. The assignment involves working with multiple datasets, I choose to work with arrest data and population data for counties in USA.

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/nogibjj/AfagR_DE_assignment6.git
   ```

2. Install the required dependencies:

   Make sure you have a Python environment set up. You can install the necessary dependencies using the \`requirements.txt\` file (if included), or manually install libraries like \`pandas\`, \`sqlalchemy\`, and \`dotenv\`.

   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Loading Data
To load data from a CSV file into a cloud-based database, use the provided script in the repository. The script connects to the database using environment variables such as \`SERVER_HOSTNAME\`, \`ACCESS_TOKEN\`, and \`HTTP_PATH\`. You need to ensure that these environment variables are set correctly in your \`.env\` file.
Additionally, because we have limited capacity I used some subset of the dataset, that's why final query results can be skewed and not displaying true information. 

### Running the Query

The repository contains SQL queries to analyze and process data related to violent crime arrests and population size.
To run the query, use the following command:

```bash
python query.py general_query
```


## Queries Explanation

The main query in this repository performs an analysis by joining two datasets:
- **Arrest Data**: Contains records of arrests made in California counties in 2009.
- **Population Data**: Contains population estimates for those same counties.

### Query Breakdown

The SQL query performs the following steps:
1. **Join Arrest Data and Population Data**: The query joins the arrest data (\`ar805_arrest_db\`) with population data (\`ar805_population_db\`) using the \`county\` field.
   
2. **Aggregation**: The query groups the data by state and sums the total population and the number of violent arrests in each state. And calculates a violence percentage in the states. 

3. **Filtering and Sorting**: The query filters out counties with null values for violent arrests and sorts the results by the number of violent arrests in descending order.



### Expected Results

- **State-wise Aggregation**: The query provides a summary of the total population and the total number of violent arrests for each state.
- **Ranked by Arrests**: The results are ordered by the number of violent arrests, with states having the highest violent crime rates appearing at the top.
  
The query helps normalize the number of violent arrests by considering the population size, making it easier to compare crime rates across counties.

![query_result](query_log.md)
