## :warning: Please read these instructions carefully and entirely first
* Clone this repository to your local machine.
* Use your IDE of choice to complete the assignment.
* When you have completed the assignment, you need to  push your code to this repository and [mark the assignment as completed by clicking here](https://app.snapcode.review/submission_links/dc1e0d71-757c-4a75-9e6d-b3319b026a1d).
* Once you mark it as completed, your access to this repository will be revoked. Please make sure that you have completed the assignment and pushed all code from your local machine to this repository before you click the link.

**Table of Contents**
1. [Before you start](#before-you-start), a brief explanation for the exercise and software prerequisites/setup.  
2. [Tips for what we are looking for](#tips-on-what-were-looking-for) provides clear guidance on solution qualities we value 
3. [The Challenge](#begin-the-two-part-challenge) explains the data engineering code challenge to be tackled.
4. [Follow-up Questions](#follow-up-questions) related to the challenge which you should address  
5. [Your approach and answers to follow-up questions](#your-approach-and-answers-to-follow-up-questions-) is where you should include the answers to the follow-up question and clarify your solution approach any assumptions you have made.

## Before you start
### Why complete this task?

We want to make the interview process as simple and stress-free as possible. That’s why we ask you to complete 
the first stage of the process from the comfort of your own home.

Your submission will help us to learn about your skills and approach. If we think you’re a good fit for our 
network, we’ll use your submission in the next interview stages too.

### About the task

You’ll be creating an ingestion process to ingest files containing vote data. You’ll also create a means to 
query the ingested data to determine outlier weeks.

There’s no time limit for this task, but we expect it to take less than 2 hours.

### Setup Instructions

For instructions on how to set up and run the exercise, please see [SETUP.md](SETUP.md).

### Tips on what we’re looking for


* ✅  **Test coverage**

    Your solution must have good test coverage, including common execution paths.

* ✅  **Self-contained tests**

    Your tests should be self-contained, with no dependency on being run in a specific order.

* ✅  **Simplicity**

    We value simplicity as an architectural virtue and a development practice. Solutions should reflect the difficulty of the assigned task, and shouldn’t be overly complex. We prefer simple, well tested solutions over clever solutions. 

    Please avoid:

   * ❌ unnecessary layers of abstraction
   * ❌ patterns
   * ❌ custom test frameworks
   * ❌ architectural features that aren’t called for
   * ❌ libraries like **pandas** or **polars** or frameworks like **PySpark** or **ballista**
  
     We know that this exercise can be
     solved fairly trivially using these libraries and a Dataframe approach, and we'd encourage appropriate 
     use of these in daily work contexts. But for this small exercise we really 
     want to know more about how you structure, write and test your Python code,
     and want you to show some fluency in SQL -- a **pandas**
     solution won't allow us to see much of that.

* ✅  **Self-explanatory code**

    The solution you produce must speak for itself. Multiple paragraphs explaining the solution is a sign 
    that the code isn’t straightforward enough to understand on its own.
    However, please do explain your non-obvious _choices_ e.g. perhaps why you decided to load
    data a specific way.

* ✅ **Demonstrate fluency with data engineering concepts**
   
   Even though this is a toy exercise, treat DuckDB as you would an OLAP 
   data warehouse. Choose datatypes, data loading methods, optimisations and data models that 
   are suited for resilient analytics processing at scale, not transaction processing.

* ✅ **Dealing with ambiguity**

    If there’s any ambiguity, please add this in a section at the bottom of the README. 
    You should also make a choice to resolve the ambiguity and proceed.

Our review process starts with a very simplistic test set in the `tests/exercise_tests` folder which you should also
check before submission. You can run these with:
```shell
poetry run exercise check-ingestion
poetry run exercise check-outliers
```

Expect these to fail until you have completed the exercise.

You should not change the `tests/exercise-tests` folder and your solution should be able to pass both tests.


## Download the dataset for the exercise
Run the command
```
poetry run exercise fetch-data
```

which will fetch the dataset, uncompress it and place it in `uncommitted/votes.jsonl` for you.
Explore the data to see what values and fields it contains (no need to show how you explored it).

## Begin the two-part challenge
There are two parts to the exercise, and you are expected to complete both. 
A user should be able to execute each task independently of the other. 
For example, ingestion shouldn't cause the outliers query to be executed.

### Part 1: Ingestion
Create a schema called `blog_analysis`.
Create an ingestion process that can be run on demand to ingest files containing vote data. 
You should ensure that data scientists, who will be consumers of the data, do not need to consider 
duplicate records in their queries. The data should be stored in a table called `votes` in the `blog_analysis` schema. 

### Part 2: Outliers calculation
Create a view named `outlier_weeks`  in the `blog_analysis` schema. It will contain the output of a SQL calculation for which weeks are regarded as outliers based on the vote data that was ingested.
The view should contain the year, week number and the number of votes for the week _for only those weeks which are determined to be outliers_, according to the following rule:

NB! If you're viewing this Markdown document in a viewer
where the math isn't rendering, try viewing this README in GitHub on your web browser, or [see this pdf](docs/calculating_outliers.pdf).

> 
> **A week is classified as an outlier when the total votes for the week deviate from the average votes per week for the complete dataset by more than 20%.**</br>  
> For the avoidance of doubt, _please use the following formula_: 
>  
> > Say the mean votes is given by $\bar{x}$ and this specific week's votes is given by $x_i$. 
> > We want to know when $x_i$ differs from $\bar{x}$ by more than $20$%. 
> > When this is true, then the ratio $\frac{x_i}{\bar{x}}$ must be further from $1$ by more than $0.2$, i.e.: </br></br> 
> > $\big|1 - \frac{x_i}{\bar{x}}\big| > 0.2$

The data should be sorted in the view by year and week number, with the earliest week first.

Running `outliers.py` should recreate the view and 
just print the contents of this `outlier_weeks` view to the terminal - don't do any more calculations after creating the view.

## Example

The sample dataset below is included in the test-resources folder and can be used when creating your tests.

Assuming a file is ingested containing the following entries:

```
{"Id":"1","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-02T00:00:00.000"}
{"Id":"2","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"4","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"5","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"6","PostId":"5","VoteTypeId":"3","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"7","PostId":"3","VoteTypeId":"2","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"8","PostId":"4","VoteTypeId":"2","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"9","PostId":"2","VoteTypeId":"2","CreationDate":"2022-01-23T00:00:00.000"}
{"Id":"10","PostId":"2","VoteTypeId":"2","CreationDate":"2022-01-23T00:00:00.000"}
{"Id":"11","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-30T00:00:00.000"}
{"Id":"12","PostId":"5","VoteTypeId":"2","CreationDate":"2022-01-30T00:00:00.000"}
{"Id":"13","PostId":"8","VoteTypeId":"2","CreationDate":"2022-02-06T00:00:00.000"}
{"Id":"14","PostId":"13","VoteTypeId":"3","CreationDate":"2022-02-13T00:00:00.000"}
{"Id":"15","PostId":"13","VoteTypeId":"3","CreationDate":"2022-02-20T00:00:00.000"}
{"Id":"16","PostId":"11","VoteTypeId":"2","CreationDate":"2022-02-20T00:00:00.000"}
{"Id":"17","PostId":"3","VoteTypeId":"3","CreationDate":"2022-02-27T00:00:00.000"}
```

Then the following should be the content of your `outlier_weeks` view:


| Year | WeekNumber | VoteCount |
|------|------------|-----------|
| 2022 | 0          | 1         |
| 2022 | 1          | 3         |
| 2022 | 2          | 3         |
| 2022 | 5          | 1         |
| 2022 | 6          | 1         |
| 2022 | 8          | 1         |

**Note that we strongly encourage you to use this data as a test case to ensure that you have the correct calculation!**

## Follow-up Questions

Please include instructions about your strategy and important decisions you made in the README file. You should also include answers to the following questions, please make sure these are answered without the use AI tools as we would like to understand your thought process:

1. What kind of data quality measures would you apply to your solution in production?
2. What would need to change for the solution scale to work with a 10TB dataset with 5GB new data arriving each day?
3. Please tell us in your modified README about any assumptions you have made in your solution (below).


## Your Approach and answers to follow-up questions 

_Please provide an explaination to your implementation approach and the additional questions **here**_

### Approach

1. First I check if the required arguments (file path) are provided or not.
2. If the file path is provided, I check if the file is present or not.
3. Then I read the file and validate if the required columns are present or not.
4. If all columns are present, I then create a stage table by:
   - Casting the columns into proper data types.
   - Creating missing columns with default values.
5. Once the stage table is created, I merge the data from the stage table to the main `votes` table.
6. Finally, I create the outliers view from the main table and print the same. 

### Answers to follow-up questions:

1. What kind of data quality measures would you apply to your solution in production?
   - I would add a more robust schema validation check, something like Great Expectations, to ensure the necessary columns are present and are of the correct data type.
   - Now, if a row in the input file doesn't have a necessary column, I'm rejecting the entire file. Instead of this, I would reject just that one row and ingest the rest of the file.
   - I would have some validation around the timestamp column. For example, to check if a record is from the future or if it's older than what's expected. This is based on the business use case.
   - If there are reference tables for UserId or PostId or VoteTypeId, I would make sure the values for these columns in the input file are valid against these tables.
   - I would also have a monitoring system to notify if any of thees issues occur during ingestion.
2. What would need to change for the solution scale to work with a 10TB dataset with 5GB new data arriving each day?
   - I would keep the data in a cloud warehouse solution such as BigQuery.
   - I would keep the input JSON files as is in a data lake (GCS or S3), but use Parquet in the actual pipeline.
   - I would partition the `votes` table by ingestion date or load date so that incremental load logic can be used.
   - I would have the outlier view as a matrialized view or a table to avoid recalculation each time we need that data.
   - I would also create an aggregated table which would hold the year, week number, and the total votes for that week, so that this also doesn't have to be calculated every time. This can be partitioned by year and week, and can be computed daily or weekly.
   - I would also explore the opportunities to stream the data for ingestion, or read the file as chunks during ingestion.
3. Please tell us in your modified README about any assumptions you have made in your solution (below).
   - I have assumed that all the ID fields (Id, UserId, PostId) are strings, to accommodate for UUIDs.
   - I have assumed that the Id field is the primary key, and used that to identify and maintain unique records.
   - I have assumed UserId is optional, as I don't see the field being present for several records. I assumed this would be present only in cases where a user has logged in before voting. 
   - I have assumed BountyAmount also to be optional.
   - I have assumed that there are 52 weeks in a year, and that the week number starts from 0 instead of 1.

## AI Tool Usage

While we encourage the use of AI tools as part of the learning process but to ensure transparency, please provide the following information regarding the use of AI tools in this submission:

1.  **Specific Use Cases:** Describe for what purposes tool was used (e.g., code generation, debugging assistance, query generation etc.).
    - I used AI to explore DuckDB methods available, and auto-completion.
2.  **Percentage of Code Generated by AI:** Provide an estimate of the percentage of the submitted code that was generated by AI.
    - Around 5%, for DuckDB methods.
3.  **How AI-Generated Code Was Reviewed:** Explain how you reviewed and verified the AI-generated code to ensure its correctness and quality.
    - After getting AI suggestion on the methods to use, I referred documentations to understand the method.
    - Manually verified from DuckDB CLI on how the methods changed the data in the tables.
    - Added unit tests and integration tests to test the same.

Please note, during the technical interview, which will build upon this exercise, we'll focus on your coding abilities and problem-solving skills without the use of AI tools. This will allow us to see your direct approach and thought process.
