CREATE KEYSPACE spark_15 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE spark_15.players (player text PRIMARY KEY, goals int);

INSERT INTO spark_15.players (player, goals) VALUES ('Diafra Sakho',9);	
INSERT INTO spark_15.players (player, goals) VALUES ('Andy Carroll',5);	
INSERT INTO spark_15.players (player, goals) VALUES ('Stewart Downing',5);
INSERT INTO spark_15.players (player, goals) VALUES ('Enner Valencia',4);
INSERT INTO spark_15.players (player, goals) VALUES ('Morgan Amalfitano',3);
INSERT INTO spark_15.players (player, goals) VALUES ('Carlton Cole',3);
INSERT INTO spark_15.players (player, goals) VALUES ('James Collins',1);
INSERT INTO spark_15.players (player, goals) VALUES ('Aaron Cresswell',1);

CREATE TABLE spark_15.cbe (instrid text PRIMARY KEY, isin text, eligCls map<text,text>, eligHc map<text,int>, eligReason map<text,text>);

INSERT INTO spark_15.cbe 
(instrid, isin , eligCls, eligHc, eligReason)
VALUES ('1234', 'AB123', {'ECB' : 'Y', 'SNB' : 'N', 'BOE' : 'Y', 'BOC' : 'Y'}, {'ECB' : 10,  'BOE' : 5, 'BOC' : 7}, {'ECB' : 'abcd',  'BOE' : 'def', 'BOC' : 'ghi'}); 

INSERT INTO spark_15.cbe 
(instrid, isin , eligCls, eligHc, eligReason)
VALUES ('1235', 'GD345', {'BOC' : 'Y', 'BOE' : 'Y'}, {'BOC' : 10,  'BOE' : 9}, {'BOC' : 'jkl',  'BOE' : 'mno'}); 

INSERT INTO spark_15.cbe 
(instrid, isin , eligCls, eligHc, eligReason)
VALUES ('1236', 'FG777', {'SNB' : 'Y'}, {'SNB' : 12}, {'SNB' : 'pqr'}); 

INSERT INTO spark_15.cbe 
(instrid, isin , eligCls, eligHc, eligReason)
VALUES ('1237', 'TY318', {'SNB' : 'Y', 'BOC' : 'Y'}, {'SNB' : 6,  'BOC' : 10}, {'SNB' : 'stu',  'BOC' : 'vw'}); 

INSERT INTO spark_15.cbe 
(instrid, isin , eligCls, eligHc, eligReason)
VALUES ('1239', 'AB777', {'ECB' : 'Y'}, {'ECB' : 10}, {'SNB' : 'xyz'}); 