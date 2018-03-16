########################################
Without user defined function:
########################################

1   rec = load '/Data/March15/weatherPIG.txt' using TextLoader as (data:chararray);
2   data_row = foreach rec generate TRIM(SUBSTRING(data,6,14)),TRIM(SUBSTRING(data,46,53)), TRIM(SUBSTRING(data,38,45));
3   Store data_row into '/Output/weatherpig' using PigStorage(',');
4   new_rec = load '/Output/weatherpig/part-m-00000' using PigStorage(',') as (date:chararray, min:double,max:double);
5   X = filter new_rec by max>25;
6   Y = filter new_rec by min<0;
7   H1 = group new_rec all;
8   I = foreach H1 generate MAX(new_rec.max) as maximum;
9   Highest = filter new_rec by max == I.maximum;
10   L1 = group new_rec all;
11   I1 = foreach L1 generate MIN(new_rec.min) as minimum;
12   lowest = filter new_rec by min == I1.minimum;


###############################################
With user defined function:
###############################################
register udf_corrupt.jar;
1   rec = load '/Data/March15/weatherPIG.txt' using TextLoader as (data:chararray);
2   rec = load '/Data/March15/weatherPIG.txt' using TextLoader as (data:chararray);
3   data_row = foreach rec generate TRIM(SUBSTRING(data,6,14)),UDF.pigudf(TRIM(SUBSTRING(data,46,53))), UDF.pigudf(TRIM(SUBSTRING(data,38,45)));
4   Store data_row into '/Output/weatherpig1' using PigStorage(',');
5   new_rec = load '/Output/weatherpig1/part-m-00000' using PigStorage(',') as (date:chararray, min:double,max:double);
6   X = filter new_rec by max>25;
7   Y = filter new_rec by min<0;
8   H1 = group new_rec all;
9   I = foreach H1 generate MAX(new_rec.max) as maximum;
10   Highest = filter new_rec by max == I.maximum;
11   L1 = group new_rec all;
12   I1 = foreach L1 generate MIN(new_rec.min) as minimum;
13   lowest = filter new_rec by min == I1.minimum;

