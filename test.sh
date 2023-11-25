#!/usr/bin/env bash

docker compose up -d

cargo test

docker compose down
