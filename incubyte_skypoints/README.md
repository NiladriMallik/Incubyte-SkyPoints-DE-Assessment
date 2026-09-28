# Incubyte SkyPoints DE Assessment

## Problem Statement:
I run a global airline loyalty program called SkyPoints, with lounges and partner airlines across the world. Every member enrolled in the program is issued a Membership Card that lets them access any partner lounge 
worldwide, redeem miles, and track their tier status.

## Current Status:
We maintain all members in one database. There are millions of members enrolled in the program. So, I decided to split up the members based on the country and load them into corresponding country tables.
To pull the members as per Country, my developers should know what are all the places the Member Data is available. So, the data extraction will be done by our Source System. It will pull all the relevant member data 
and give us two feeds every day: a flat file of member profile data, and a semi-structured JSON feed of mileage redemption transactions from our partner airlines.

## Technical Assessment: Deliverables
1. Create table queries – DDL for the raw/landing table, the staging table, and the country-specific target 
tables (e.g., in Snowflake).
2. Load the staging table with additional derived columns: Age (computed from DOB) and a Stale_Member 
flag where days since Flight_Date > 90.
3. Write the transformation logic (SQL and/or Python) to split members into their per-country target tables, 
applying the “latest record wins” rule when a member has moved countries.
4. Parse the semi-structured JSON redemption feed into a flattened, queryable table, and describe how you 
would join it back to the member profile data.
5. Create the necessary data validations — mandatory field checks, key-column uniqueness, and any checks 
you would add to catch the kind of data issues visible in the sample data above.
6. If we move forward with an interview, we would like to see a live demonstration.