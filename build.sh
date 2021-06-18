#!/bin/sh
set -e

EXPLORER_VERSION=8.1.0


echo "Fetching explorer $EXPLORER_VERSION source"
curl -L https://github.com/ergoplatform/explorer-backend/archive/refs/tags/$EXPLORER_VERSION.tar.gz > explorer-backend-$EXPLORER_VERSION.tar.gz

echo "Extracting explorer source"
rm -rf explorer-backend/$EXPLORER_VERSION
mkdir explorer-backend/$EXPLORER_VERSION
tar -xf explorer-backend-$EXPLORER_VERSION.tar.gz
mv explorer-backend-$EXPLORER_VERSION explorer-backend/$EXPLORER_VERSION/explorer-backend
rm explorer-backend-$EXPLORER_VERSION.tar.gz

echo "Preparing explorer Dockerfile"
cp explorer-backend/$EXPLORER_VERSION/explorer-backend/modules/chain-grabber/Dockerfile explorer-backend/$EXPLORER_VERSION/chain-grabber.Dockerfile

echo "Done."