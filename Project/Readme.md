# Objective

Automatic Creation Of Indices

# Details

In this project, our aim was to modify the access layer of PostgreSQL to keep track of
relation scans that could have benefitted from an index, and if there are many such
scans for a particular index, create the index automatically.

# Summary of implementation

First, we check if there is a need for index creation. Then, we check whether the ratio of
the number of tuples satisfying the where clause and the total number of tuples in the
table is less than a certain threshold.We do that only if the execution process undergoes
sequential scan. If so, the index is created.

# Files Included :

- patchfile.diff - This contains the difference between 2 files postgres.c and nodeSeqscan.c from the original postgres code.
- projectreport.pdf - Details of why and where the changes were made to the base code
