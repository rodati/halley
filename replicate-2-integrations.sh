node . -c collections-2-integrations.yml -n 3 \
--mongo "mongodb://halley-staging:6lTZBVfBqLA0BESc@integrations-shard-00-00-enyvz.mongodb.net:27017,integrations-shard-00-01-enyvz.mongodb.net:27017,integrations-shard-00-02-enyvz.mongodb.net:27017/local?ssl=true&replicaSet=integrations-shard-0&authSource=admin&readPreference=secondary" \
--sql "postgres://sirena:d8WFUyycHGdzyonAhyYKzBoV@sirena2.csioptwgy8uh.us-east-1.rds.amazonaws.com:5432/sirena"
