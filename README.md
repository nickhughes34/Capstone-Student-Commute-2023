# Capstone Study Project | Group 15: Student Commute Analysis
**Client/Supervisor:** Dr. Khalil El-Khatib, Faculty of Business & IT (FBIT)

**Project Team:** Nicholas Hughes *(Team Lead)*, Matthew Denniston, Anoje Janathanan, Franklin Muhuni

**Project Goal:** The purpose of this project was to address the issue of long commute times to campus. Through the analysis of routes and location, we sought to identify ways to decrease the average commute time (which we found to be roughly 70 minutes) by roughly 10 minutes.

# Project Dependencies
### Tools and Architecture:
<p align="center">
  <img width="682" height="149" src="https://i.imgur.com/A4HGA4d.png">
</p>

- Python, Google Maps Platform (Integration)
- PostgresSQL (Storage)
- Excel (Visualization)

### Data Sources:
The data we used consisted of two semesters of student enrolment data provided to us by the school and consisted of some of the following attributes:
- Term Code
- Student ID
- City & Postal Code
- Class times & Schedules
- Campus Location

In accordance with the agreement our team signed at the begin of the project, we have refrained from publishing our initial dataset or any subsequent findings.

### Database ERD:
![ERD](https://i.imgur.com/0wpa7tv.png)

# Project Files
For more information or insight regarding these files see the [second branch](https://github.com/nickhughes34/Capstone-Student-Commute-2023/tree/main) of this repository where our entire process is documented in the README.
- ***ParseExcel.py*** - Parses through initial dataset, cleaning and prepping the file as input to query Google Maps Platform API.
- ***StudentCommute.py*** - Takes output from ParseExcel.py, requests route data from Google Maps and appends entries to DB.
- ***Query.py files*** - Defines the functions used in StudentCommute.py to query data from the local DB.
- ***Heatmaps.ipynb*** - Creates heat map visualizations of where students begin their commute to campus.
