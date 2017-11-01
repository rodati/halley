node . -c collections-1-core.yml -n 3 \
--mongo "mongodb://cosmo-postgre:ficus@ds061542-a1.mongolab.com:61542,ds061542-a0.mongolab.com:61542/local?replicaSet=rs-ds061542&authSource=cosmo-production&readPreference=secondary" \
--sql "postgres://sirena:d8WFUyycHGdzyonAhyYKzBoV@sirena2.csioptwgy8uh.us-east-1.rds.amazonaws.com:5432/sirena"
