#!/bin/bash

# Function to display usage instructions
usage() {
    echo "Usage: ./install_osrm.sh <CONTAINER_FILE_NAME> <PORT_NUMBER> <WGET_URL>"
    echo "Example: ./install_osrm.sh india_osrm_container 5000 https://download.geofabrik.de/asia/india-latest.osm.pbf"
    exit 1
}

# Check if the correct number of arguments is provided
if [ "$#" -ne 3 ]; then
    echo "Error: Incorrect number of arguments."
    usage
fi

# Assigning arguments to variables
OSRM_SETUP_DIR="data/osrm_setup"
CONTAINER_FILE_NAME=$1
PORT_NUMBER=$2
WGET_URL=$3

# Create a directory for the OSRM setup
mkdir -p $OSRM_SETUP_DIR
cd $OSRM_SETUP_DIR

# Step 1: Download OSM data
echo "Downloading OSM data from ${WGET_URL}..."
wget ${WGET_URL} -O ${CONTAINER_FILE_NAME}.pbf

# Step 2: OSRM extraction
echo "Extracting OSM data using OSRM..."
sudo docker run --rm -t -v "$(pwd)":/data ghcr.io/project-osrm/osrm-backend:v5.27.1 osrm-extract -p /opt/car.lua /data/${CONTAINER_FILE_NAME}.pbf

# Step 3: OSRM partition
echo "Partitioning OSM data..."
sudo docker run --rm -t -v "$(pwd)":/data ghcr.io/project-osrm/osrm-backend:v5.27.1 osrm-partition /data/${CONTAINER_FILE_NAME}.osrm

# Step 4: OSRM customization
echo "Customizing OSRM data..."
sudo docker run --rm -t -v "$(pwd)":/data ghcr.io/project-osrm/osrm-backend:v5.27.1 osrm-customize /data/${CONTAINER_FILE_NAME}.osrm

# Step 5: OSRM docker deployment
echo "Deploying OSRM with Docker..."
sudo docker run -t -i -p ${PORT_NUMBER}:5000 -v "$(pwd)":/data -d --name ${CONTAINER_FILE_NAME} ghcr.io/project-osrm/osrm-backend:v5.27.1 osrm-routed --thread 14 --algorithm mld --max-table-size 100000000 --max-trip-size 100000000 /data/${CONTAINER_FILE_NAME}.osrm

# Step 6: Stopping OSRM container
echo "Stopping OSRM container. To start OSRM container from now on, use: sudo docker start ${CONTAINER_FILE_NAME}"
sudo docker stop ${CONTAINER_FILE_NAME}
echo "OSRM setup completed!"