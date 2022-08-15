# BigQuery Porter

BigQuery Porter is a tool to manage BigQuery Metadata and Deployment.

## Get Started

### Installation

```
npm i bigquery-porter
```

Set up your OAuth and GCP Default Project.

### Clone BigQuery resources from your GCP Project

First step to start to use is cloning BigQuery resources as files in remote.
Next comamnd, you'll get BigQuery metadata files and DDL codes in local filesystem.

```sh
npx bqport pull --all --with-ddl @default
```

Typical directory strucuture follows:

```
bigquery: Root Directory
`-- @default: Project Name
    `-- sandbox
        |-- @models
        |   `-- mymodel
        |       `-- metadata.json
        |-- @routines
        |   |-- sample_function
        |   |   |-- ddl.sql
        |   |   `-- metadata.json
        |   |-- sample_procedure
        |   |   |-- ddl.sql
        |   |   `-- metadata.json
        |   |-- sample_table
        |   |   |-- ddl.sql
        |   |   `-- metadata.json
        |   `-- sample_tvf
        |       |-- ddl.sql
        |       `-- metadata.json
        |-- ddl.sql
        |-- metadata.json
        |-- sample_materialized_view
        |   |-- ddl.sql
        |   |-- metadata.json
        |   `-- schema.json
        |-- sample_table
        |   |-- ddl.sql
        |   |-- metadata.json
        |   `-- schema.json
        `-- sample_view
            |-- metadata.json
            |-- schema.json
            `-- view.sql
`-- <other_projects>
```

#### Deploy Your BigQuery Resources

After you modifeid files in locals, you can deploy them by next command:
The following command will deploy to the default project in GCP.

```
npx bqport push
```

`bqport` deploys codes in prallel and topological order by SQL's DAG dependencies.


#### Partial Deployment from STDIN

You can also deploy your BigQuery queries from a list of SQL files.
This is useful for cases where you want to execute only specific SQL.
For example, a diff execution by git.

```
find ./bigquery -name '*.sql' | npx bqport push
```
