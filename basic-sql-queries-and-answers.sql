#1 Which are the different genres?**
SELECT DISTINCT a.prime_genre 
FROM applestore.data a;

#2 Which is the genre with more apps rated?**
SELECT a.prime_genre, COUNT(a.id)
FROM applestore.data a
WHERE a.rating_count_tot > 0
GROUP BY a.prime_genre
ORDER BY 2 DESC
LIMIT 1;
#games

#3 Which is the genre with more apps?**
SELECT a.prime_genre, COUNT(a.id)
FROM applestore.data a
GROUP BY a.prime_genre
ORDER BY 2 DESC
LIMIT 1;
#games

#4 Which is the one with less?**

SELECT a.prime_genre, COUNT(a.id)
FROM applestore.data a
GROUP BY a.prime_genre
ORDER BY 2 ASC
LIMIT 1;
#catalogs

#5 Take the 10 apps most rated.**
SELECT a.track_name, a.rating_count_tot
FROM applestore.data a
ORDER BY 2 DESC
LIMIT 10;


#6 Take the 10 apps best rated by users.**
SELECT a.track_name, a.user_rating, a.rating_count_tot
FROM applestore.data a
ORDER BY a.user_rating DESC, a.rating_count_tot DESC
LIMIT 10;


#7Take a look on the data you retrieved in the question 5. Give some insights.**
'''
data retrieved in question 5 returned the apps who had been rated or voted on the most. The results are consistent with my experience with applestore. 
Facebook, instagram and clash of clans make sense being 1,2 and 3 given my knowledge of their popularity.
note the query just returned those apps with the most amount of ratings from users - it does not reflect the quality of those ratings, ie scores. 
'''

#8. Take a look on the data you retrieved in the question 6. Give some insights.**
'''
initially my query returned the apps with the top 10 ratings by score. 
However given the data type is integer, and there are 490 ids with a rating of 5, I felt this did not provide much inisght by just listing 10 apps with a rating of 5. 
Therefore I decided to also include the number of ratings/votes in my query. ordering by first the rating and then the number of ratings gave more weight to the apps
which had been voted on more times. In my opinion this is a truer reflection of the best rated apps.

'''

#9. Now compare the data from questions 5 and 6. What do you see?**
'''
Without taking into account the rating counts in my question 6 answer. The results for top 10 by number of votes, and top 10 by rating scores
were completely different. This is because just ordering by rating scores gave apps with a low number of votes. 
This is why i decided to include number of votes into my answer. 

'''

#10. How could you take the top 3 regarding the user ratings but also the number of votes?**
SELECT a.track_name, a.user_rating, a.rating_count_tot
FROM applestore.data a
ORDER BY a.user_rating DESC, a.rating_count_tot DESC
LIMIT 3;

#11. Does people care about the price?** Do some queries, comment why are you doing them and the results you retrieve. What is your conclusion?

#avg price per user rating score, cross checked to make sure number of votes has statistical significance
SELECT a.user_rating, AVG(a.price), COUNT(a.rating_count_tot)  
FROM applestore.data a
GROUP BY a.user_rating
ORDER BY 1 DESC;
'''
based on the above query results I do not see a relation between the pricing of the app and the quality of the rating score it received.
'''
#number of ratings/votes per price bracket.
SELECT a.price, SUM(a.rating_count_tot)  
FROM applestore.data a
GROUP BY a.price
ORDER BY 1 ASC;

'''
given the number of ratings given(votes) is the only indicator we have for the quantity of usage for each app, I thought it would make sense to look at how many votes were placed
for each price as a proxy for app usage figures.
the results demonstrated that the majority of votes placed/ratings given were for those apps priced as free or in the single digits
'''
SELECT a.prime_genre, AVG(a.price), SUM(a.rating_count_tot), AVG(a.user_rating)
FROM applestore.data a
GROUP BY 1
ORDER BY AVG(a.user_rating) desc, avg(a.price) desc;

'''
I wanted to see a breakdown by genre, whether average rating score was affected by the average price of apps in that genre. 
from what I can infer, there is no indication that average price has an effect on the average rating score. for example,
productivity genre has the best average rating score but also an average price of $4.33 which is relatively high. 
Another example, News has an average rating score of 2.98 and average price of $0.52
'''

'''
my conclusion is that it depends on what determines as """do people care"""? 
if the quality of the user score is meant by this statement, my answer is I cannot determine any impact from price on the average score an app receives.
If tot_rating_count can be interpreted as a proxy for app usage, then yes there is an indication that price has an impact on how much people use the app. 
'''



