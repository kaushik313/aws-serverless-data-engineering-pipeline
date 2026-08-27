SELECT area_location,
COUNT(*) AS total_restaurants
FROM zomato_db.area_summary
GROUP BY area_location
ORDER BY total_restaurants DESC;
