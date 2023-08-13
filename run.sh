#!/bin/sh


target/release/rusts3 -b "./test-data" --bind-address "192.168.44.172" --bind-port 18000 --log-conf log4rs.yml
