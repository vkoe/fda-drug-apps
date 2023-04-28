ALTER TABLE company
ADD parent INTEGER;

UPDATE company
SET parent = (SELECT parent_id 
              FROM bridge
              WHERE  bridge.self_id = company.id);

PRAGMA foreign_keys = OFF;
	DELETE FROM company
	WHERE NOT id = parent;

ALTER TABLE company
DROP parent;