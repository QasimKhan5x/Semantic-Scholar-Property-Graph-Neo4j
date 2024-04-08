#!/bin/bash

# Example of setting environment variables for sensitive data
export NEO4J_URI=neo4j://localhost:7687
# prompt for username
read -p 'Enter Neo4j Username: ' NEO4J_USER \n
export NEO4J_USER
# It's better to prompt for passwords or use a secure vault
read -sp 'Enter Neo4j Password: ' NEO4J_PASSWORD \n
export NEO4J_PASSWORD
read -p 'Enter Neo4j Database: ' NEO4J_DATABASE \n
export NEO4J_DATABASE

# Assuming your Python script is named loader.py
python loader.py "$@"
python PartBKhanPaudel.py "$@"
python PartCKhanPaudel.py "$@"
python PartDKhanPaudel.py "$@"

# Clear the password variable for security
unset NEO4J_PASSWORD
