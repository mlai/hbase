umask 022
mvn -q clean
mvn -DskipTests install assembly:assembly

