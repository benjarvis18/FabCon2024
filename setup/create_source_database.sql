CREATE TABLE dbo.Employee (
    EmployeeID INT IDENTITY(1,1) PRIMARY KEY,
    FirstName varchar(100) NOT NULL,
    LastName varchar(100) NOT NULL,
    Email varchar(255) UNIQUE NOT NULL,
    Phone varchar(15),
    HireDate DATE NOT NULL,
    JobTitle varchar(100),
    Department varchar(100),
    IsActive BIT DEFAULT 1
);


CREATE TABLE dbo.Client (
    ClientID INT IDENTITY(1,1) PRIMARY KEY,
    ClientName varchar(255) NOT NULL
)

CREATE TABLE dbo.Project (
    ProjectID INT IDENTITY(1,1) PRIMARY KEY,
    ClientID INT  FOREIGN KEY REFERENCES Client(ClientID),
    ProjectName varchar(255) NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE,
    Status varchar(50),  -- E.g., 'Active', 'Completed', 'On Hold'
    Budget DECIMAL,
    Description varchar(MAX)
);

CREATE TABLE dbo.ProjectAssignment (
    ProjectAssignmentID INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT FOREIGN KEY REFERENCES Employee(EmployeeID),
    ProjectID INT FOREIGN KEY REFERENCES Project(ProjectID),
    Role varchar(100),  -- E.g., 'Developer', 'Project Manager', 'Consultant'
    AssignmentStartDate DATE NOT NULL,
    AssignmentEndDate DATE
);

CREATE TABLE dbo.TimeLog (
    TimeLogID INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT FOREIGN KEY REFERENCES Employee(EmployeeID),
    ProjectID INT FOREIGN KEY REFERENCES Project(ProjectID),
    LogDate DATE NOT NULL,
    HoursWorked DECIMAL NOT NULL,  -- E.g., 8.00 hours
    DayRate DECIMAL NOT NULL,  -- E.g., $500.00
    BilledAmount DECIMAL NOT NULL,  -- E.g., $4000.00
    Description varchar(MAX)
);