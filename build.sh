#/bin/bash

version="1.3"

echo "Building HyperGraphDB tarball, version $version..."

echo "Create fresh dist directory"
rm -rf dist
mkdir dist
mkdir dist/lib
mkdir dist/apidocs
mkdir dist/src
mkdir dist/src/hgdb
mkdir dist/src/hgdbp2p
mkdir dist/src/hgbdbje
mkdir dist/ThirdPartyLicensing

echo "Build and Copy Core Module jar"
cp "core/target/hgdb-$version.jar" dist/lib

echo "Build and Copy P2P Module jar"
cp "p2p/target/hgdbp2p-$version.jar" dist/lib

echo "Build and Copy BDB-JE Storage Module jar"
cp "storage/bdb-je/target/hgbdbje-$version.jar" dist/lib

echo "Generating API Javadocs"
javadoc -d dist/apidocs -sourcepath core/src/java:p2p/src/java:storage/bdb-je/src/java -subpackages org

echo "Copy licensing files"
cp -r core/LicensingInformation core/ThirdPartyLicensing dist

echo "Copy source files"
cp -r core/src/java core/src/config dist/src/hgdb
cp -r p2p/src/* dist/src/hgdbp2p
cp -r storage/bdb-je/src/* dist/src/hgbdbje

echo "Copy readme.html"
cp readme.html dist

echo "Create zip file"
mv dist hypergraphdb-1.3
rm hgdbdist-1.3-final.zip hgdbdist-1.3-final.tar.gz
zip hgdbdist-1.3-final.zip hypergraphdb-1.3/*
tar cvzf hgdbdist-1.3-final.tar.gz hypergraphdb-1.3