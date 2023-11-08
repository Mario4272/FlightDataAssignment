# FlightDataAssignment

## Introduction

Welcome to the FlightDataAssignment! This exercise is designed to evaluate your programming skills in a real-world context. Your task is to perform data analysis using Spark/Scala, providing insights from the provided flight data. If you progress to the interview stage, be prepared to discuss your approach and solution.

## Prerequisites

- Git
- IntelliJ IDEA
- Scala Build Tool (SBT)

## Setup

1. Clone the repository from GitHub to your local machine.
2. Open the cloned directory in IntelliJ IDEA.
3. Build the project using IntelliJ's build features.

## Running the Application

Upon running the solution, an interactive menu will guide you through various analysis tasks:

![image](https://github.com/Mario4272/FlightDataAssignment/assets/19327923/82101490-b1b6-4431-9997-3380fe9eaba0)


Each option corresponds to a question from the assignment described below. Select the desired option and follow any additional prompts to view the results.

## Assignment Details

### Data Provided

You will work with two CSV files:

- `flightData.csv` containing passenger ID, flight ID, departure country, destination country, and flight date.
- `passengers.csv` containing passenger ID, first name, and last name.

### Questions

Your task is to answer the following questions using the provided data:

1. **Total Flights per Month:** Compute the total number of flights for each month.
2. **Frequent Flyers:** Identify the top 100 most frequent flyers.
3. **International Travels without UK:** Determine the greatest number of countries a passenger has been to without visiting the UK.
4. **Common Flyers:** Find pairs of passengers who have been on more than 3 flights together.

### Additional Challenge

As an extra task, calculate the pairs of passengers who have flown together more than 'N' times within a specific date range.

### Function Example

```scala
def flownTogether(atLeastNTimes: Int, from: Date, to: Date) = {
  // Your code here
}

