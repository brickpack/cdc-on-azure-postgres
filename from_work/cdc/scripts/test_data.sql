

INSERT INTO toolbox."domain"
(id, is_business, created_at, updated_at)
VALUES(nextval('domain_id_seq'::regclass), false, '2026-06-03 13:40:18.000', '2026-06-03 13:40:18.000');

INSERT INTO toolbox.user_in_domain (id,profile_id,domain_id,is_admin,is_blocked,created_at,updated_at) VALUES
	 (29,23,9694473,true,false,'2022-02-05 12:43:54','2022-02-05 12:43:54'),
	 (30,130,17313340,true,false,'2021-11-24 10:30:45','2021-11-24 10:30:45'),
	 (31,221,9832231,true,false,'2016-10-26 10:15:17','2016-10-26 10:15:17'),
	 (32,609,18015724,true,false,'2023-08-16 11:04:46','2023-08-16 11:04:46'),
	 (33,693,10899323,true,false,'2021-12-31 14:59:49','2021-12-31 14:59:49');

UPDATE toolbox."domain" SET updated_at = current_timestamp WHERE id < 32;
UPDATE toolbox."user" SET updated_at = current_timestamp WHERE id = 37;

DELETE FROM toolbox."domain" WHERE id < 25;
DELETE FROM toolbox."user"  WHERE id = 38;


UPDATE cdc.debezium_heartbeat SET ts = now() WHERE id = 1;


SELECT COUNT(*) FROM toolbox.host;
SELECT COUNT(*) FROM toolbox.host_to_universal_key;
SELECT COUNT(*) FROM  toolbox."domain";
SELECT COUNT(*) FROM  toolbox."subscription";
SELECT COUNT(*) FROM  toolbox.universal_key;
SELECT COUNT(*) FROM  toolbox.user_in_domain;
