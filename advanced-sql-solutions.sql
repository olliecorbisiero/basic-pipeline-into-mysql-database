#challenge 1 - USING DERIVED TABLES

#step 1, royalties on each sale, 
#note each title may appear more than once because multiple sales

SELECT ta.au_id, 
t.title_id, 
s.qty AS sales_volume, 
t.price, 
t.royalty/100 AS royalty, 
ta.royaltyper/100 AS royaltyper,
ROUND(s.qty * t.price * (t.royalty/100) * (ta.royaltyper/100),2) AS sales_royalty

FROM sales s
INNER JOIN titleauthor ta
ON s.title_id = ta.title_id
INNER JOIN titles t 
ON ta.title_id = t.title_id
ORDER BY ta.au_id;



#step two, royalties + advance for each title for each author

SELECT ta.au_id, 
t.title_id, 
SUM(s.qty) AS sales_volume, 
t.price, 
t.royalty/100 AS royalty, 
ta.royaltyper/100 AS royaltyper,
ROUND(SUM(s.qty) * t.price * (t.royalty/100) * (ta.royaltyper/100),2) AS sales_royalty,
t.advance as total_advance,
ROUND(t.advance*(ta.royaltyper/100),2) AS author_advance

FROM sales s
INNER JOIN titleauthor ta
ON s.title_id = ta.title_id
INNER JOIN titles t 
ON ta.title_id = t.title_id
GROUP BY ta.au_id, t.title_id
ORDER BY ta.au_id;

#step 3 total profits of each author

SELECT au_id, SUM(sales_royalty)+ SUM(author_advance) AS author_profits
FROM 
(SELECT ta.au_id, 
t.title_id, 
SUM(s.qty) AS sales_volume, 
t.price, 
t.royalty/100 AS royalty, 
ta.royaltyper/100 AS royaltyper,
ROUND(SUM(s.qty) * t.price * (t.royalty/100) * (ta.royaltyper/100),2) AS sales_royalty,
t.advance as total_advance,
ROUND(t.advance*(ta.royaltyper/100),2) AS author_advance

FROM sales s
INNER JOIN titleauthor ta
ON s.title_id = ta.title_id
INNER JOIN titles t 
ON ta.title_id = t.title_id
GROUP BY ta.au_id, t.title_id
ORDER BY ta.au_id) AS salesroyalties

GROUP BY au_id
ORDER BY 2 DESC
LIMIT 3;


#ALTERNATIVE SOLUTION - USING TEMP TABLES


CREATE TEMPORARY TABLE salesroyalties
SELECT ta.au_id, 
t.title_id, 
SUM(s.qty) AS sales_volume, 
t.price, 
t.royalty/100 AS royalty, 
ta.royaltyper/100 AS royaltyper,
ROUND(SUM(s.qty) * t.price * (t.royalty/100) * (ta.royaltyper/100),2) AS sales_royalty,
t.advance as total_advance,
ROUND(t.advance*(ta.royaltyper/100),2) AS author_advance

FROM sales s
INNER JOIN titleauthor ta
ON s.title_id = ta.title_id
INNER JOIN titles t 
ON ta.title_id = t.title_id
GROUP BY ta.au_id, t.title_id
ORDER BY ta.au_id;


#top 3 orders by profit 
SELECT au_id, SUM(sales_royalty)+ SUM(author_advance) as author_profits
FROM salesroyalties
GROUP BY au_id
ORDER BY 2 DESC
LIMIT 3;

#Challenge 3, permanent table 'most_profiting_authors'


CREATE TABLE most_profiting_authors
AS 
SELECT au_id, SUM(sales_royalty)+ SUM(author_advance) as profits
FROM salesroyalties
GROUP BY au_id;

SELECT * FROM most_profiting_authors;


