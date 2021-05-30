#Challenge 1 - Who Have Published What At Where?
SELECT a.au_id as "AUTHOR ID", 
a.au_lname as "LAST NAME", 
a.au_fname as "FIRST NAME",
t.title as "TITLE", 
p.pub_name as "PUBLISHER"
FROM authors a 
INNER JOIN titleauthor ta on a.au_id = ta.au_id
INNER JOIN titles t on ta.title_id = t.title_id
INNER JOIN publishers p on t.pub_id = p.pub_id
ORDER by a.au_id;

#Challenge 2 - Who Have Published How Many At Where?
SELECT a.au_id as "AUTHOR ID", 
a.au_lname as "LAST NAME", 
a.au_fname as "FIRST NAME", 
p.pub_name as "PUBLISHER", 
count(t.title_id) as "TITLE COUNT"
FROM authors a 
INNER JOIN titleauthor ta on a.au_id = ta.au_id
INNER JOIN titles t on ta.title_id = t.title_id
INNER JOIN publishers p on t.pub_id = p.pub_id
GROUP BY a.au_id, p.pub_id
ORDER by  count(t.title_id) DESC;

#checking the results
create temporary table sumcheck1
SELECT a.au_id as "AUTHOR ID", 
a.au_lname as "LAST NAME", 
a.au_fname as "FIRST NAME", 
p.pub_name as "PUBLISHER", 
count(t.title_id) as title_count
FROM authors a 
INNER JOIN titleauthor ta on a.au_id = ta.au_id
INNER JOIN titles t on ta.title_id = t.title_id
INNER JOIN publishers p on t.pub_id = p.pub_id
GROUP BY a.au_id, p.pub_id
ORDER by  count(t.title_id) DESC;

Select sum(title_count)
from sumcheck1;

Select count(ta.title_id) from titleauthor ta;

#Challenge 3 - Best Selling Authors

SELECT a.au_id as "AUTHOR ID", 
a.au_lname as "LAST NAME", 
a.au_fname as "FIRST NAME", 
sum(s.qty) as TOTAL
FROM authors a 
INNER JOIN titleauthor ta on a.au_id = ta.au_id
INNER JOIN titles t on ta.title_id = t.title_id
INNER JOIN sales s on t.title_id = s.title_id
GROUP BY a.au_id
ORDER by TOTAL DESC
LIMIT 3;

#Challenge 4 - Best Selling Authors Ranking

SELECT a.au_id as "AUTHOR ID", 
a.au_lname as "LAST NAME", 
a.au_fname as "FIRST NAME", 
COALESCE(sum(s.qty),0) as TOTAL
FROM authors a 
LEFT JOIN titleauthor ta on a.au_id = ta.au_id
LEFT JOIN titles t on ta.title_id = t.title_id
LEFT JOIN sales s on t.title_id = s.title_id
GROUP BY a.au_id
ORDER by TOTAL DESC
;



