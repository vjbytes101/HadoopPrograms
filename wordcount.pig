rec = LOAD '/Data/March5/wordcountproblem' as (line);
words = foreach rec generate flatten(TOKENIZE(line)) as word;
grpword = group words by word;
cntword = foreach grpword generate group,COUNT(words);
dump cntword;
