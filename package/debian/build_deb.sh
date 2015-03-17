#!/bin/bash
set -e

echo "Present working directory: `pwd`"

sbt job-server/assembly

PKG=spark-job-server.$$
mkdir -p $PKG
echo "Using package directory: $PKG"

mkdir -p $PKG/opt/spark-job-server

cp job-server/target/spark-job-server.jar $PKG/opt/spark-job-server

cp bin/server_start.sh $PKG/opt/spark-job-server
cp bin/server_stop.sh $PKG/opt/spark-job-server
cp package/debian/settings.sh $PKG/opt/spark-job-server
cp package/debian/service $PKG/opt/spark-job-server
cp config/log4j-server.properties $PKG/opt/spark-job-server
cp config/job-server-default.conf $PKG/opt/spark-job-server

cd $PKG
fpm -s dir -t deb -a all \
    --verbose \
    --name spark-job-server \
    --template-value date="$DATE" \
    --template-value gitCommit="$GIT_COMMIT" \
    --depends 'oracle-jre' \
    --before-install ../package/debian/before_install.sh \
    --after-install ../package/debian/after_install.sh \
    --before-remove ../package/debian/before_remove.sh \
    --after-remove ../package/debian/after_remove.sh \
    .

cd ..

mv $PKG/spark-job-server*deb job-server/target/

rm -rf $PKG

exit $?

