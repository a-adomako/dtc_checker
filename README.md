# DTC Website Checker

## Purpose:
Scans a list of URLs (CSV) to detect if a website is a DTC (direct-to-consumer) e-commerce store and what tech stack it uses.

How It Works
	1.	Reads input CSV containing a url column.
	2.	Fetches each site using httpx with pacing, retries, and user-agent rotation.
	3.	Falls back to Playwright for JS-heavy or blocked sites.
	4.	Parses HTML to detect:
	  •	Platforms (Shopify, Woo, BigCommerce, Magento)
	  •	DTC and e-commerce signals (cart, checkout, products)
	  •	Subscriptions, BNPL, email/SMS, loyalty, and quizzes
	5.	Scores each site and marks it as:
	  •	TRUE → DTC detected
	  •	FALSE → no signals found
	  •	BLOCKED → site blocked or failed both passes
	6.	Writes results to a new CSV (with checkpoints).

## Usage
python3 dtc_checker.py --input input.csv --output results.csv

## Optional flags:
--concurrency (default 5), --timeout (20s), --threshold (5)

Notes
	•	Produces partial checkpoint files every 25 URLs.

Creator Notes
	•	Did not work as expected — some pages fail detection or remain marked as BLOCKED despite being accessible.
