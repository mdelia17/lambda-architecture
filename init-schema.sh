#!/bin/bash


cqlsh -e "create keyspace if not exists dns with replication = {'class':'SimpleStrategy', 'replication_factor':1}; 
create table if not exists dns.nameserver (address inet primary key, packets_sent counter, packets_received counter);
create table if not exists dns.domain (domain text primary key, requests counter);
create table if not exists dns.info (type text primary key, packets counter);
create table if not exists dns.cname (cname text primary key, count counter);
create table if not exists dns.ns (ns text primary key, count counter);
create table if not exists dns.conversation (key <text,text,text> primary key, messages list<text>);
truncate dns.nameserver; 
truncate dns.domain;
truncate dns.info;
truncate dns.cname;
truncate dns.ns;
truncate dns.conversation;
exit;"