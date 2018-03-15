i/p

en google.com 50 100
en yahoo.com 60 100
us google.com 70 100
en google.com 68 100

Script to compute pagecount of each domain having language 'en'

records = LOAD '/Data/March15/mydata.txt' using PigStorage(' ') as (lang:chararray, pagename:chararray, pagecount:int, pagesize:int);
filter_record = FILTER records by lang=='en';
group_record = GROUP filter_record by pagename;
res = FOREACH group_record generate group,SUM(filter_record.pagecount);
sorted_res = ORDER res by $1 desc;
store sorted_res INTO '/Output/pagecount1';
