--Load data from HDFS
inputDialouges4 = LOAD 'hdfs:///user/chandra/inputs/episodeIV_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
inputDialouges5 = LOAD 'hdfs:///user/chandra/inputs/episodeV_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
inputDialouges6 = LOAD 'hdfs:///user/chandra/inputs/episodeVI_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);

--Filer out the first 2 lines from each file
ranked4 = RANK inputDialouges4;
OnlyDialouges4 = FILTER ranked4 BY (rank_inputDialouges4 > 2);
ranked5 = RANK inputDialouges5;
OnlyDialouges5 = FILTER ranked5 BY (rank_inputDialouges5 > 2);
ranked6 = RANK inputDialouges6;
OnlyDialouges6 = FILTER ranked6 BY (rank_inputDialouges6 > 2);

--Merge the three inputs
inputData = UNION OnlyDialouges4, OnlyDialouges5, OnlyDialouges6;

--Group by name
groupByName = GROUP inputData By name;

--Count the number of lines by each character
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;

--Remove the outpit folder
rmf hdfs:///user/chandra/outputs;

--Store result in HDFS
STORE namesOrdered INTO 'hdfs:///user/chandra/outputs' USING PigStorage('\t');