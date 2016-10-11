elastic-mover
=============

An easy-to-use, single-file, PHP CLI script tool for dump, restore, save, import, export, migrate Elasticsearch Indices from elasticsearch to file, from file to elasticsearch, or from elasticsearch to elasticsearch.

### Usage

```sh
php elasticmover.php -i=<INPUT PATH> -o=<OUTPUT PATH> [options...]
```
INPUT PATH and OUTPUT PATH can be:
- elasticsearch index url: `{protocol}://{host}:{port}/{index}`
- file path: `/path/to/file`

### Options
- `-d` export or import index data (default)
- `-m` export or import index map

### Example
Export an index data from elasticsearch server to file:
```sh
php elasticmover.php -i=http://localhost:9200/index -o=./index.data -d
```
Export an index map from elasticsearch server to file:
```sh
php elasticmover.php -i=http://localhost:9200/index -o=./index.map -m
```
Import an index data from file to elasticsearch server:
```sh
php elasticmover.php -i=./index.data -o=http://localhost:9200/newindex -d
```
Import an index map from file to elasticsearch server:
```sh
php elasticmover.php -i=./index.map -o=http://localhost:9200/newindex -m
```
