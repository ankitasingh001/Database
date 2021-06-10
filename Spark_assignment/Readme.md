# Spark assignment

In this assignment we have written a Spark code to process a set of files (articles) in a dataset in parallel to find co-occurrences of entities in articles.  For example, if an article mentions Narendra Modi and Rahul Gandhi, we say that the two entities co-occur. We assume that entity names are single words (Modi, Gandhi, etc), and we assume no two entities have the same name.  The list of entities to consider is at the end of this page; use it in a constant array in your program.

**Input:**

    You are provided with a set of news articles as json files in a zip file that you should expand into a directory.
    Each json file contains metadata about the article and the article content (article_body) itself.

**Objective of your code:**

In every article some entities may be mentioned.  Two different entities mentioned in an article are said to co-occur in the article. 

## Following tasks have been done

- Create a dataset with (entity, article_id) pairs where the article contains the entity.  There should not be any duplicate rows. Print the dataset.
- Using the dataset from (1) create a dataset containing (entity, count) pairs indicating how many articles the entity occurs in.  Entities that don't occur in any dataset should not be output.   Print the dataset.  You should tokenize the input, and perform case-insensitive comparison; the wordcount program example will help you with this.
- Using the dataset from (1), create a dataset with ((e1, e2), count) where count is the total number of news articles that (e1, e2) both occur in, for all entity pairs (e1, e2).   If a pair of entites do not co-occur,  you should not include them.  Print the dataset
- Using the datasets from (2) and (3), create a dataset with entity pairs (e1, e2) such that e1 and e2 occur in at least one news article, but the pair does not occur in any article.  Print the dataset.

### List of entities:

modi
rahul
jaitley
sonia
lalu
nitish
farooq
sushma
tharoor
smriti
mamata
karunanidhi
kejriwal
sidhu
yogi
mayawati
akhilesh
chandrababu
chidambaram
fadnavis
uddhav
pawar	
