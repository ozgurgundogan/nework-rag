

docker-compose up -d mongo --build
sleep 3
docker-compose up data-loader --build
sleep 3
docker-compose up relationship-updater --build
