## Compilation & Implementation

This platform was designed and developed using Java v21.  Specific details around Java included here:

- java 21.0.2 2024-01-16 LTS
- Java(TM) SE Runtime Environment (build 21.0.2+13-LTS-58)
- Java HotSpot(TM) 64-Bit Server VM (build 21.0.2+13-LTS-58, mixed mode, sharing)

This platform also uses Gradle as the package and repository management service.  Specific details around Gradle included here:

------------------------------------------------------------
Gradle 8.7
------------------------------------------------------------

Build time:   2024-03-22 15:52:46 UTC
Revision:     650af14d7653aa949fce5e886e685efc9cf97c10

Kotlin:       1.9.22
Groovy:       3.0.17
Ant:          Apache Ant(TM) version 1.10.13 compiled on January 4 2023
JVM:          21.0.2 (Oracle Corporation 21.0.2+13-LTS-58)
OS:           Windows 10 10.0 amd64

### About
This platform is to act as a backend application that Extracts, Transforms, Loads data from source systems to destination locations.  

1) your compilation & implementation platform with the version, where to download
your implementation platform, how to install and configure the platform;
2) how to compile your code;
3) how to execute your system.

### Setting Up MSSQL
1. Download SSMS from Microsoft: https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver16#download-ssms
2. Navigate through the prompts to install the latest version of SSMS
3. Download SQL Server: https://www.microsoft.com/en-us/sql-server/sql-server-downloads
4. Follow the prompts for SQL Server, install a basic installation
    1. Note your key installation credentials like Instance Name, Administrators, and Connection String
    2. The team's example is as follows: MSSQLSERVER01, DESKTOP-BBB6R7K\EV-04, Server=localhost\MSSQLSERVER01;Database=master;Trusted_Connection=True;
5. Launch the SQL Server Configuration Manager
6. Under SQL Server Network Configuration, locate Protocols for <Instance Name>
7. Change the TCP/IP from Disabled to Enabled
8. Perform any machine resets as required
9. Launch SSMS and put localhost as the Server name
10. In localhost, create a new database called medical
11. Right click on medical, go to tasks and select Import Data
12. Hit next on the first prompt
13. Data Source will be selected as Flat File
14. Navigate to the root project directory
15. The file location will be under data/ called all_prevelances.csv
16. Select this file as the data source file
    1. On the left hand side, click Advanced
    2. Click ITEM and change the DataType from String to "text stream"
17. When prompted for the destination, choose SQL Server Native Client 11.0
18. Fill in the Server Name details - the Server Name should be localhost, Authentication keep at Windows Auth, Database will be medical
19. In Next, Edit Mappings.  Ensure ITEM is set to varchar and the chracter length is "max"
20. Execute the import Wizard query

### Setting Up PostGres
1. Install Postgres (PgAdmin) in your computer. Use port 5432 and postgres1 as password for postgres – i.e. username/password – postgres/postgres1 - Postgres server 12 will be fine. (https://www.youtube.com/watch?v=0n41UTkOBb0 )
2. Pull/Clone the health-api repository: https://github.com/danielolaya12/heatlh-api
3. In pgadmin, navigate and login to the Postgres Server created in step 1
4. Under Databases, create a new database, called DiabetesDB
5. Copy and paste the following Postgres SQL Script into Postgres SQL Developer to create the database and upload data
    ```
    BEGIN TRANSACTION;
    -- Database: DiabetesDB
    -- DROP DATABASE IF EXISTS "DiabetesDB";
    DROP TABLE IF EXISTS patient_data;
    CREATE TABLE patient_data (
        gender VARCHAR(10),
        age float,
        hypertension INTEGER,
        heart_disease INTEGER,
        smoking_history VARCHAR(20),
        bmi NUMERIC(5,2),
        HbA1c_level NUMERIC(4,2),
        blood_glucose_level INTEGER,
        diabetes INTEGER
    );

    COPY patient_data(gender, age, hypertension, heart_disease, smoking_history, bmi, HbA1c_level, blood_glucose_level, diabetes)
    FROM '<PATH>/diabetes_prediction_dataset.csv'
    WITH (FORMAT CSV, HEADER);

    COMMIT TRANSACTION;
    ```
6. In the script above, replace '<PATH>' with the exact path to othe root of the health-api project clone/pull
    For example: "C:\Users\EV-04\Documents\SMU\7319\project\heatlh-api\diabetes_prediction_dataset.csv" would be an example path
7. Execute the script

### Setting Up API
1. Ensure that all steps for PostGres were set up and implemented properly
2. Run the HealthApiApplication.java or run the compiled class from the "health-api" github repository: https://github.com/danielolaya12/heatlh-api
3. Test connection with http://localhost:8080/api/patients

### Setting Up CSV Location
1. Ensure "Independent_Medical_Reviews.csv" is in the root of this project

### Setting Up S3 Buckets
1. Create a Free Tier AWS Account
2. When logged in, navigate to S3 by searching for S3 in the AWS Console Search Bar
3. Under General Purpose Bucket, click the Orange icon labeled "Create Bucket"
4. Name this bucket "cs7319"
    1. Disable ACLs
    2. Uncheck Block All Public Access
    3. Disable Bucket Versioning
    4. Keep default settings for Encryption
5. Search for IAM in the AWS Console Search Bar
6. On the left hand panel, select Users
7. Create User
8. Name the User with desired name - for this project we called the user cs7319_Project_User, click Next
9. In Permissions options, set to Add user to group
10. In User groups, no group should exist.  Create Group
11. Name the Group with desired name - for this project we called the group cs7319_project_group
12. Search for AmazonS3FullAccess in the Permissions Policies
13. Check this policy, click Create User Group
14. In the Create User window, click the check box next to the newly created user group, click Next
15. Click Create User
16. Navigate to the newly created user by clicking Users on the left panel and clicking on the User name
17. In the Security Credentials tab, scroll down to the Access Keys section
18. Create access key
19. Specify Local Code as the Key Practice
20. Check the confirmation box at the bottom, click Next
21. Describe the purpose - for this project we said Credentials for CS7319 Project
22. Create access Key
23. Save and download this access key in the provided .csv format
24. Navigate back to s3
25. Navigate to inside the cs7319 bucket
26. Create folder
27. Enter "base" as the name
28. Leave server-side encryption as "Do not specify an encryption key"
29. Repeat steps 26 - 29 for "curated", "inbound", and "schema_log" folders
30. Click inside "base"
31. Create folder
32. Enter "medical_c2" as the name
33. Leave server-side encryption as "Do not specify an encryption key"
34. Repeat stpes 30 - 33 for the following names:
    1. medical_pf
    2. operations_pf
    3. patients_pf
    4. regulatory_pf
    5. trials_pf
    6. operations_c2
    7. patients_c2
    8. regulatory_c2
    9. trials_c2
35. Click inside "curated"
36. Repeat steps 31 - 34
37. Click inside schema_log
38. Repeat steps 31 - 34
39. Click inside inbound
40. Upload
41. Drag and drop "medications.csv" into the next screen and submit the Upload
    1. medications.csv can be found in the data folder under the root of the project

### Building Dependencies
1. In Command Prompt, navigate to this project location
2. Execute the gradle build command "gradle build"

### Modifying code to specific setup
1. Within Main.java, replace lines 15 and 16 with your own AWS Credentials and defined in the S3 buckets settings
2. This step can be bypassed by requesting a set of access and secret keys from the project team (who have their own S3 set up)
    1. Keys are only required, there are no special permissions on accounts
    2. Although the team can provide credentials upon request, there will need to be coordination if verification is required to view the contents of the buckets
3. In MSSQLReader.java, replace line 16 with the respective DB_URL
    1. The team used a local machine and had this string as the DB_URL: jdbc:sqlserver://DESKTOP-BBB6R7K;databaseName=medical;integratedSecurity=true;trustServerCertificate=true
    2. Most likely, others wanting to implement this will need to replace DESKTOP-BBB6R7K with the specified local user as created in "Setting Up MSSQL".  Most likely this will be replaced with "localhost"

## Architectural Style

#### Pipe and Filter
The team identified the following as Pros for the Pipe and Filter architecture:

- The Pipe and Filter architecture allowed the team to reuse software components with ease
- This is apparent when reusing both our s3Reader.java and s3Writer.java classes
- The architecture allowed for our code to be modularized and our Main function follows this approach, making it easy for us to scale up if more data sources are required
- The methods used in Pipe and Filter allow for concurrency without risk of affecting data ACIDity
- Simplicity in development; Pipe and Filter methods were simple to develop as general logic was straightforward given the architectural design
- Each method does a specific, unique task offering separation of concerns
- Simple connectors

However, there are a list of cons the team considered:

- The team found it difficult to handle errors; it seems that there were multiple different areas where specific errors had to be handled and there was no general way to handle these errors effectively
- The architecture and implementation for this problem allowed for some concurrency, but there were areas in the chain of command that could not allow concurrency or else we would introduce data inconsistency
    - I.e. Base layer operations must be completed before curated layer operations
- If multiple pipe and filters were needed, the team assessed that this could be consequential for this architecture; the handling of data sources can scale relatively easily, but the introduction of exponential amounts of pipes or filters will not
- Methods are coupled; although not an explicit con, this means that certain methods are dependent on the run and execution of other methods

#### C2 Connector
- The team identified the following as Pros for the C2 architecture:
- C2 offers decoupled or very loose coupling of our components; often our components are only dependent on objects or data streams as input parameters, whereas Pipe and Filter required methods to complete and signal before the next method could execute
- The C2 development was relatively easy to refactor from the Pipe and Filter components because the core logics are the same, but the returns are different
- The team found it easier to identify areas where the code was not performing as intended due to better fault isolation inherent within C2
- Error logging was much easier and more efficient to understand and implement into C2; the team often found that there were no specialized error handles required for most of the components
- The component connectors interacted the same way because the C2 connector is interoperable

However, there are a list of cons to consider for this architecture:
- It appeared that C2 was more overhead heavy; processes would take longer compared to the Pipe and Filter architecture – we did not capture CPU usage for this, but it is an unqualified observation
- There was an increase latency with C2, and this was captured by the team as part of the Risk Analysis
- The team found that if the C2 architecture is used, it may introduce too many connectors and connecting interfaces to properly scale for a data ingestion problem
- Complex connectors

#### Architectural Decision
After consideration into the pros and cons for each architecture, and after implementing and analyzing both architectures, the team narrowed on using Pipe and Filter as the architecture of choice.  The team centered and concluded this decision on three key differentiators:

- Pipe and Filter is easier and more intuitive to implement than C2 for a data ingestion problem
- Future scalability is a main requirement for most data ingestion exercises – we believe P&F offers a better scalability option
- Concurrency implementation in methods offers a more efficient approach to data ingestions

