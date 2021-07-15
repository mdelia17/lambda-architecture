#!/bin/bash

docker run --rm --network net-cassandra bitnami/cassandra:3.11.10 cqlsh -e "create keyspace if not exists dns with replication = {'class':'SimpleStrategy', 'replication_factor':1};create table if not exists dns.nameserver (address inet primary key, packets_sent counter, packets_received counter);create table if not exists dns.domain (domain text primary key, requests counter);create table if not exists dns.info (type text primary key, packets counter);create table if not exists dns.cname (cname text primary key, count counter);create table if not exists dns.ns (ns text primary key, count counter);create table if not exists dns.conversation (key tuple<text,text,text> primary key, messages list<text>);create table if not exists dns.window_count_ns_cname_domain (start timestamp, end timestamp, type text, domain text, requests counter, primary key (start, end, type, domain));create table if not exists dns.window_address (start timestamp, end timestamp, address inet, packets_sent counter, packets_received counter, primary key (start, end, address));CREATE TABLE IF NOT EXISTS dns.byte_sum_window (start timestamp, end timestamp, sum counter, count counter, PRIMARY KEY (start, end));CREATE TABLE IF NOT EXISTS dns.type_window (start timestamp, end timestamp, type text, count counter, PRIMARY KEY (start, end, type));truncate dns.nameserver;truncate dns.domain;truncate dns.info;truncate dns.cname;truncate dns.ns;truncate dns.conversation;truncate dns.window_count_ns_cname_domain;truncate dns.window_address;truncate dns.byte_sum_window;truncate dns.type_window;exit;" --username cassandra --password cassandra cas-node1
