-- Creating Dataset ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE social_media (
	Text VARCHAR,
	Sentiment VARCHAR,
	Timestamp VARCHAR,
	User_name VARCHAR,
	Platform VARCHAR,
	Hashtags VARCHAR,
	Retweets NUMERIC,
	Likes NUMERIC,
	Country VARCHAR,
	Year NUMERIC,
	Month NUMERIC,
	Day NUMERIC,
	Hour NUMERIC
) 

-- Import Data

-- Structuring ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET datestyle = DMY;
ALTER TABLE social_media
ALTER COLUMN timestamp TYPE TIMESTAMP
	USING (timestamp::TIMESTAMP)

ALTER TABLE social_media
RENAME COLUMN timestamp to post_date

-- delete 433 rows which the post_date were not in 2023
DELETE FROM social_media
WHERE year <> 2023

-- add column post_id, trim(text, sentiment, platform, country and hashtags) to remove space
CREATE TABLE media 
AS (
SELECT 	ROW_NUMBER() OVER(ORDER BY post_date) as post_id,
    		TRIM(text) as text,
    		TRIM(sentiment) as sentiment,
    		post_date,
    		user_name,
    		TRIM(platform) as platform,
    		TRIM(hashtags) as hashtags,
    		retweets,
    		likes,
    		TRIM(country) as country,
    		year, month, day, hour
FROM social_media
	)

DROP TABLE social_media

-- Analysis ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Top 10 Sentiment 
SELECT sentiment, COUNT(sentiment) as amount
FROM media
GROUP BY sentiment
ORDER BY amount DESC
LIMIT 10

-- Top Country (sentiment positive + most likes)
WITH top_country AS (
SELECT 	country, SUM(likes) as like_amount,
		COALESCE
		((SELECT COUNT(sentiment)
		FROM media
		WHERE sentiment = 'Positive'
			AND country = a.country
		GROUP BY country)
		,0) as positive_amount,
		COUNT(post_id) as post_amount
FROM media as a
GROUP BY country
ORDER BY like_amount DESC
)
SELECT	country,
		like_amount,
		ROUND(
			CAST(like_amount AS DECIMAL) / post_amount
			,2) as like_per_post,
		positive_amount,
		ROUND(
			100.00 * positive_amount / post_amount
			,2) as positive_rate,
		post_amount
FROM top_country

-- Top Platform
WITH top_platform AS (
SELECT 	platform, 
	COUNT(post_id) as post_amount, 
	SUM(likes) as like_amount,
	COALESCE
	((SELECT COUNT(sentiment)
	FROM media
	WHERE sentiment = 'Positive'
		AND platform = a.platform
	GROUP BY platform)
	,0) as positive_amount
FROM media as a
GROUP BY platform
ORDER BY like_amount DESC
)
SELECT	platform,
		like_amount,
		ROUND(
			CAST(like_amount AS DECIMAL) / post_amount
			,2) as like_per_post,
		positive_amount,
		ROUND(
			100.00 * positive_amount / post_amount
			,2) as positive_rate,
		post_amount
FROM top_platform

-- which time of the week has the most like
WITH step_1 AS (
SELECT 	EXTRACT(DOW FROM post_date) as DOW,
		hour,
		SUM(likes) as total_likes
FROM media
GROUP BY DOW, hour
ORDER BY DOW, hour
)
SELECT 	CASE
		WHEN DOW = 0 THEN 'Sunday'
		WHEN DOW = 1 THEN 'Monday'
		WHEN DOW = 2 THEN 'Tuesday'
		WHEN DOW = 3 THEN 'Wednesday'
		WHEN DOW = 4 THEN 'Thursday'
		WHEN DOW = 5 THEN 'Friday'
		WHEN DOW = 6 THEN 'Saturday'
	END as DOW,
	hour, total_likes
FROM step_1

-- top hashtags
WITH seperate AS (
SELECT 	post_id, likes, hashtags,
		REGEXP_SPLIT_TO_TABLE(hashtags, ' ') as hashtag	
FROM media
)
SELECT	hashtag,
		SUM(likes) as like_per_hashtag
FROM seperate
GROUP BY hashtag
ORDER BY like_per_hashtag DESC









