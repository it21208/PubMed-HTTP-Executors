#!/bin/bash

#
# EXAMPLE Usage 1
#
#  From within the PROJECT/scripts folder issue the following command:
#
#  time ./runPubMedHTTPFetch.sh -Xmx8g test.txt 400 2 /home/alex/Desktop/output/
#
# This command will use as input file the "test.txt", epost batch size 400, 2 threads and use as output directory /home/alex/Desktop/output/
# The command will also report the duration of the execution.


# find the directory where the script is located in
BASE="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#echo "BASE = $BASE"

Xmx=${1}
InputFile=${2}
PostBatchSize=${3}
Threads=${4}
OutputDir=${5}

# define the configuration file for the Apache LOG4J framework
LOG4J_CONFIGURATION=${BASE}/../src/main/resources/log4j.properties
#echo "LOG4J_CONFIGURATION = $LOG4J_CONFIGURATION"

# define the JVM options/parameters
JAVA_OPTS="${Xmx} -Dlog4j.configuration=file:${LOG4J_CONFIGURATION}"
#echo "JAVA_OPTS = $JAVA_OPTS"

# change to the ../target directory to more easily create the classpath
cd ${BASE}/../target
#pwd
# define the class path
CLASS_PATH="$(for file in `ls -1 *.jar`; do myVar=$myVar./$file":"; done;echo $myVar;)"
#echo "CLASS_PATH1 = ${CLASS_PATH}"
CLASS_PATH="./classes:${CLASS_PATH}"
#echo "CLASS_PATH2 = ${CLASS_PATH}"

# define the executing-main class
MAIN_CLASS="gr.uoa.di.rdf.pubmed_http.FetchPubMedArticles"

EXEC="java $JAVA_OPTS -cp $CLASS_PATH $MAIN_CLASS -input ${InputFile} -batch ${PostBatchSize} -threads ${Threads} -outdir ${OutputDir}"

eval ${EXEC}

cd ${BASE}
