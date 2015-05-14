# Spark Examples
This is a small collection of examples that run on Apache Spark.

## Testing
Tests are included for all examples. Unit testing is an integral part of the
programming process, and in the words of Kent Beck:

> If a feature does not have a test, it does not exist

## AverageNGramLength
[Google NGrams](http://storage.googleapis.com/books/ngrams/books/datasetsv2.html)
tracks the occurrences of various words and phrases throughout all the books in
Google Books by year. The 1-Grams dataset tracks single words. The 1-grams data
is tab-separated and its schema is:

1. word - The word of interest
2. year - The year of the data
3. count - The number of times the word has appeared in books in this year
4. volumes - The number of volumes the words has appeared in for this year

The output from `com.alectenharmsel.examples.spark.AverageNGramLength` is
comma-separated and in the form:

1. year - The year of interest
2. length - The average length of all words in the year

This output can be easily plotted with your plotting tool of choice. I
personally like R.

## MoabLicenseInfo
Moab is a scheduler from Adaptive Computing. It can record software license
usage data in its logs. I'm not including any sort of instructions for using
this, as it is extremely special-purpose.