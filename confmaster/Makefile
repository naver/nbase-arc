all:
	mvn package -Dmaven.test.skip=true

test:
	mvn clover2:setup -Pintegrated-test test -e

gcov:
	mvn package -Dmaven.test.skip=true
	# Need clover license for opensource projects. 
	# Temporarily commented out until getting the license.
	#mvn clover2:setup -Pintegrated-test package -Dmaven.test.skip=true -e

clean:
	mvn clean
