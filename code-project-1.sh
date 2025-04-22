#0: Download data
curl -O https://raw.githubusercontent.com/yinghaoz1/tmdb-movie-dataset-analysis/master/tmdb-movies.csv

#1:
csvsort -c release_date -r tmdb-movies.csv > release_date_sorted.csv

#2:
csvgrep -c vote_average -r '^(7\.[6-9]|[89])' tmdb-movies.csv > vote_average_filtered_above_7.5.csv

#3:
#Highest revenue movie
csvsort -c revenue -r tmdb-movies.csv | head -n 2 > highest_revenue_movie.csv

#Lowest revenue movie

min_revenue=$(csvsql --tables tmdb --query "SELECT MIN(revenue) FROM tmdb" "tmdb-movies.csv" | tail -n +2 | tr -d '"')

csvsql --tables tmdb --query "SELECT * FROM tmdb WHERE revenue = $min_revenue" "tmdb-movies.csv" > "lowest_revenue_movies.csv"

#4:
csvcut -c revenue tmdb-movies.csv | tail -n +2 | awk '{sum+=$1} END {print "Total revenue: " sum}'

#5
csvsql --tables tmdb --query "SELECT *, revenue - budget AS profit FROM tmdb" clean_tmdb.csv > movies_with_profit.csv

csvsort -c profit -r movies_with_profit.csv | head -n 11 > top_10_profit_movies.csv

#6
#Top director
csvcut -c director tmdb-movies.csv | tail -n +2 | grep -v '^$' | grep -v '^""$' | sort | uniq -c | sort -nr | head -n 1

#Top actor
csvcut -c cast tmdb-movies.csv | tail -n +2 | grep -v '^$' | grep -v '^""$' | tr '|' '\n' | grep -v '^$' | grep -v '^""$' | sort | uniq -c | sort -nr | head -n 1

#7
csvcut -c genres tmdb_movies.csv.csv | tail -n +2 | tr '|' '\n' | sort | uniq -c | sort -nr > genre_count.txt

