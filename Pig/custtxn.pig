1   cust = LOAD '/Data/March12/custs' using PigStorage(',') as (custid:chararray, custfname:chararray, custlname:chararray, age:long, profession: chararray);
2   odrec = limit cust 100;
3   grp = group cust by profession;
4   num_cust = foreach grp generate group, COUNT(cust);
5   trans = LOAD '/Data/March12/txns' using PigStorage(',') as (txnid:chararray, date:chararray, custid:chararray, amount:double, category:chararray, product:chararray,state:chararray, type:chararray);
6   grptxn = group trans by custid;
7   totalsp = foreach grptxn generate group, SUM(trans.amount);
8   custorder = order totalsp by $1 desc;
9   topcust = limit custorder 100;
10   cust = LOAD '/Data/March12/custs' using PigStorage(',') as (custid:chararray, custfname:chararray, custlname:chararray, age:long, profession: chararray);
11   odrec = limit cust 100;
12   grp = group cust by profession;
13   num_cust = foreach grp generate group, COUNT(cust);
14   trans = LOAD '/Data/March12/txns' using PigStorage(',') as (txnid:chararray, date:chararray, custid:chararray, amount:double, category:chararray, product:chararray,state:chararray, type:chararray);
15   grptxn = group trans by custid;
16   totalsp = foreach grptxn generate group, SUM(trans.amount);
17   custorder = order totalsp by $1 desc;
18   topcust = limit custorder 100;
19   topcustjoin = join topcust by $0, cust by $0;
20   top100 = foreach topcustjoin generate $0,$3,$4,$5,$6,$1;
