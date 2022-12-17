#!/bin/bash

# Create a new Docker container for Postgres
docker run --name postgres-ds9 --network=DS9-network --hostname postgres -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres