-- 
--     internalObjectId = serializers.CharField(required=True)
--     internalName = serializers.CharField(required=False, default=None, max_length=20)
--     ra = serializers.FloatField(required=True)
--     decl = serializers.FloatField(required=True)
--     flagDate = serializers.DateTimeField(required=False, default=datetime.now())
--     counter = serializers.IntegerField(required=False, default=0)
--     survey_database = serializers.CharField(required=True, max_length=20)
--     # Override the flagDate with a discovery year. Sometimes objects are flagged late.
--     year = serializers.IntegerField(required=False, default=0)
--     # Get the original date inserted.
--     insertDate = serializers.DateTimeField(required=False, default=datetime.now())
-- 
-- | id        | int(11)   | NO   | PRI | NULL              | auto_increment |
-- | ra        | double    | NO   |     | NULL              |                |
-- | decl      | double    | NO   |     | NULL              |                |
-- | raorig    | double    | NO   |     | NULL              |                |
-- | declorig  | double    | NO   |     | NULL              |                |
-- | datefound | timestamp | NO   |     | CURRENT_TIMESTAMP |                |
-- +-----------+-----------+------+-----+-------------------+----------------+
-- 6 rows in set (0.00 sec)
-- 
-- mysql> desc othernames;
-- +--------+---------+------+-----+---------+-------+
-- | Field  | Type    | Null | Key | Default | Extra |
-- +--------+---------+------+-----+---------+-------+
-- | id     | int(11) | NO   | MUL | NULL    |       |
-- | source | text    | YES  |     | NULL    |       |
-- | name   | text    | YES  |     | NULL    |       |
-- 

select case
       when (o.id >= 900000 and o.id < 1000000) then "2009"
       when (o.id >= 1000000 and o.id < 1100000) then "2010"
       when (o.id >= 2000000 and o.id < 3000000) then "2011"
       when (o.id >= 3000000 and o.id < 4000000) then "2012"
       when (o.id >= 4000000 and o.id < 5000000) then "2013"
       when (o.id >= 5000000 and o.id < 6000000) then "2014"
       when (o.id >= 6000000 and o.id < 7000000) then "2015"
       when (o.id >= 7000000 and o.id < 8000000) then "2016"
       when (o.id >= 8000000 and o.id < 9000000) then "2017"
       when (o.id >= 9000000 and o.id < 10000000) then "2018"
       when (o.id >= 10000000 and o.id < 11000000) then "2019"
       when (o.id >= 11000000 and o.id < 12000000) then "2020"
       when (o.id >= 12000000 and o.id < 13000000) then "2021"
       when (o.id >= 13000000 and o.id < 14000000) then "2022"
       when (o.id >= 14000000 and o.id < 15000000) then "2023"
       when (o.id >= 15000000 and o.id < 16000000) then "2024"
       else NULL
       end as year,
       case
       when (o.id >= 900000 and o.id < 1000000) then (select o.id - 900000)
       when (o.id >= 1000000 and o.id < 1100000) then (select o.id - 1000000)
       when (o.id >= 2000000 and o.id < 3000000) then (select o.id - 2000000)
       when (o.id >= 3000000 and o.id < 4000000) then (select o.id - 3000000)
       when (o.id >= 4000000 and o.id < 5000000) then (select o.id - 4000000)
       when (o.id >= 5000000 and o.id < 6000000) then (select o.id - 5000000)
       when (o.id >= 6000000 and o.id < 7000000) then (select o.id - 6000000)
       when (o.id >= 7000000 and o.id < 8000000) then (select o.id - 7000000)
       when (o.id >= 8000000 and o.id < 9000000) then (select o.id - 8000000)
       when (o.id >= 9000000 and o.id < 10000000) then (select o.id - 9000000)
       when (o.id >= 10000000 and o.id < 11000000) then (select o.id - 10000000)
       when (o.id >= 11000000 and o.id < 12000000) then (select o.id - 11000000)
       when (o.id >= 12000000 and o.id < 13000000) then (select o.id - 12000000)
       when (o.id >= 13000000 and o.id < 14000000) then (select o.id - 13000000)
       when (o.id >= 14000000 and o.id < 15000000) then (select o.id - 14000000)
       when (o.id >= 15000000 and o.id < 16000000) then (select o.id - 15000000)
       else NULL
       end as counter,
       case
       when (o.source like '%telescope%' or (o.source is null and name like "CfA%")) then "CfA"
       when (o.source like '%star%/ps1/%' or ((o.source is null and name like "0%") or (o.source is null and name like "QUB0%"))) then "ps1ss"
       when (o.source like '%star%/ps1md/%') then "ps1md"
       when (o.source like '%star%/ps1fgss/%') then "ps1fgss"
       when (o.source like '%star%/ps13pi/%') then "ps13pi"
       when (o.source like '%star%/ps23pi/%') then "ps23pi"
       when (o.source like '%star%/ps1gw/%') then "ps1gw"
       when (o.source like '%star%/pso3/%') then "pso3"
       when (o.source like '%star%/ps2o3/%') then "ps2o3"
       when (o.source like '%star%/ps1ncu/%') then "ps1ncu"
       when (o.source like '%star%/ps1yse/%') then "ps1yse"
       when (o.source like 'QUB') then "ps1fgss"
       else NULL
       end as survey_database,
       case
       when (substring_index(substring_index(o.source,'/',-2),'/',1) = 'ps1' or substring_index(substring_index(o.source,'/',-2),'/',1) = 'psdb') then replace(name, 'QUB','')
       when (o.source is null) then replace(name, 'QUB','')
       when (o.source like 'QUB') then replace(name, 'QUB','')
       else substring_index(substring_index(o.source,'/',-2),'/',1)
       end as internalObjectId,
       replace(name, 'QUB','') as internalName,
       datefound as flagDate,
       datefound as insertDate,
       ra,
       decl

  from othernames o, events e where e.id = o.id order by e.id, datefound
--  limit 10000
;

