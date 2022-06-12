# BigQuery Asset Manager

BQRM is a tool to manage BigQuery Metadata and Deployment. 
Supported DAG Deployment. Try it!

## Get Started

### Installation

```
npm i <package_name>
```

Set upt your OAuth and GCP Default Project.

### Usage

#### Download BigQuery Metadata

次のコマンドは、GCP Projectのメタ情報をローカルのファイルシステムと連携します。

```
npx <package_name> pull --all --with-ddl @default
```

##### Basic Directory Structure

```
```

#### Deploy Your BigQuery Resources

次のコマンドを実行すると、GCPのデフォルトプロジェクトにデプロイされます。 この実行は並列化されており、またSQLからの依存関係を読み取り実行を行います。

```
npx <package_name> push
```

#### Partial Deployment from STDIN

SQLファイルのリストからBigQueryのクエリ実行を行うことができます。
特定のSQLのみを実行させたいケースに役立ちます。例えばgitによる差分実行などです。

```
find ./bigquery -name '*.sql' | npx <package_name> push
```
