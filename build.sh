#
# A simple sh script to run ant
#

export JAVA_HOME=/usr/java/jdk1.3.1_01

LCP=$JAVA_HOME/lib/tools.jar
for i in `ls lib/*.jar`
do
	LCP=$LCP:$i
done

java -Xmx512M -Dbuild.compiler=modern -cp $LCP org.apache.tools.ant.Main $1 $2 $3 $4
