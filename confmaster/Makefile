all:
	mvn package -Dmaven.test.skip=true

test:
	mvn clover2:setup -Pintegrated-test test -e

gcov:
	mvn clover2:setup -Pintegrated-test package -Dmaven.test.skip=true -e

clean:
	mvn clean
