#!/bin/bash

# Rename a database from one schema to a new EMPTY schema. This does NOT (and cannot) move the views.
# Recreate the views on the new database AFTER the move. The user must have full permissions on both schemas.

if [ $# -ne 3 ]
then
   echo "Usage: `basename $0` <oldschema> <newschema> <database host>"
   echo "E.g. `basename $0` atlasold atlasnew db1"
   exit 1
fi

export OLDSCHEMA=$1
export NEWSCHEMA=$2
export DBHOST=$3

echo 'SET FOREIGN_KEY_CHECKS=0;'
mysql -ukws -h${DBHOST} --skip-column-names -Be "SET SESSION group_concat_max_len=65536; SELECT GROUP_CONCAT('RENAME TABLE ${OLDSCHEMA}.', table_name, ' TO ${NEWSCHEMA}.', table_name SEPARATOR '; ') command FROM information_schema.TABLES WHERE table_schema='${OLDSCHEMA}' and table_comment != 'VIEW';" | sed -e 's/$/;/' | sed -e 's/; \+/;\n/g'
echo 'SET FOREIGN_KEY_CHECKS=1;'
