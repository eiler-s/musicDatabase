# MusicBrainz + Discogs Database
This project used the Google Cloud Platform (GCP) to combine two open-source datasets ([MusicBrainz](https://musicbrainz.org/) and [Discogs](https://www.discogs.com/about)) containing information about the music industry. After ingesting the data to GCP, each dataset was cleaned of bad data through SQL transformations and Apache Beam processing with Python. The cleaned data was modeled into tables in a database. The database was then analyzed using SQL to produce insights into the music industry and to examine the completeness of the separate data sources.

## Contributors
Nicole Currens, nc23469, nicolecurrens12@gmail.com\
Eiler Schiotz, ejs2546, eiler.schiotz@gmail.com
