--  Should list Dogs
SHOW TABLES; 

--  Reject this insert because it has a duplicate Primary Key
INSERT INTO Dogs (tag_id, first_name, gender, weight, dob) VALUES (9901, 'Fido', 'Male', 10.2, '2018-09-09');

--  Update Record
UPDATE Dogs
SET weight = 4.1
WHERE tag_id = 9090;
--  Confirm update above by viewing
SELECT * FROM Dogs WHERE tag_id = 9090;

--  Should delete two records tag_id = 11565 and tag_id = 12329
DELETE FROM Dogs WHERE dob = '2008-10-25';

--  Confirm that both are gone
SELECT * FROM Dogs WHERE tag_id = 9100 OR tag_id = 9073;


DROP TABLE Dogs;

--  Should *NOT* list Dogs
SHOW TABLES;
