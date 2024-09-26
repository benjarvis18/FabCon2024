INSERT INTO [dbo].[Employee] (FirstName, LastName, Email, Phone, HireDate, JobTitle, Department, IsActive)
VALUES 
('John', 'Doe', 'johndoe@example.com', '555-1234', '2020-01-15', 'Developer', 'IT', 1),
('Jane', 'Smith', 'janesmith@example.com', '555-5678', '2019-05-20', 'Project Manager', 'IT', 1),
('Michael', 'Brown', 'michaelbrown@example.com', '555-8765', '2021-09-12', 'Consultant', 'IT', 1),
('Sarah', 'Johnson', 'sarahjohnson@example.com', '555-4321', '2022-11-01', 'Business Analyst', 'Business', 1),
('Emily', 'Davis', 'emilydavis@example.com', '555-3456', '2018-07-23', 'Quality Assurance', 'QA', 1);

INSERT INTO [dbo].[Client] (ClientName)
VALUES
('Acme Corp'),
('Globex Corporation'),
('Initech'),
('Umbrella Corporation'),
('Stark Industries');

INSERT INTO [dbo].[Project] (ClientID, ProjectName, StartDate, EndDate, Status, Budget, Description)
VALUES 
(1, 'Website Redesign', '2023-01-10', '2023-06-30', 'Completed', 50000, 'Redesign the corporate website for Acme Corp.'),
(2, 'ERP Implementation', '2022-09-01', '2023-12-31', 'Active', 150000, 'Implement a new ERP system for Globex Corporation.'),
(3, 'Mobile App Development', '2023-03-15', NULL, 'On Hold', 75000, 'Develop a mobile app for Initech.'),
(4, 'Cybersecurity Audit', '2023-04-01', '2023-10-15', 'Completed', 30000, 'Conduct a cybersecurity audit for Umbrella Corporation.'),
(5, 'AI Integration', '2023-05-01', NULL, 'Active', 120000, 'Integrate AI into Stark Industries processes.');

INSERT INTO [dbo].[ProjectAssignment] (EmployeeID, ProjectID, Role, AssignmentStartDate, AssignmentEndDate)
VALUES 
(1, 1, 'Developer', '2023-01-10', '2023-06-30'),
(2, 2, 'Project Manager', '2022-09-01', NULL),
(3, 3, 'Consultant', '2023-03-15', NULL),
(4, 4, 'Business Analyst', '2023-04-01', '2023-10-15'),
(5, 5, 'Quality Assurance', '2023-05-01', NULL);

INSERT INTO [dbo].[TimeLog] (EmployeeID, ProjectID, LogDate, HoursWorked, Description, DayRate, BilledAmount)
VALUES 
(1, 1, '2023-01-12', 8.00, 'Worked on front-end development.', 1000, 1000),
(1, 1, '2023-01-13', 7.50, 'Continued front-end work.', 1000, 1000/7.5),
(2, 2, '2023-09-10', 8.00, 'Project planning meeting.', 1100, 1100),
(3, 3, '2023-03-17', 6.00, 'Consultation on app design.', 1100, 1100/6.0),
(4, 4, '2023-04-05', 7.00, 'Audit report preparation.', 1100, 1100/7.0),
(5, 5, '2023-05-05', 8.00, 'Test plan creation.', 1100, 1100);

-- Assigning more employees to projects
INSERT INTO [dbo].[ProjectAssignment] (EmployeeID, ProjectID, Role, AssignmentStartDate, AssignmentEndDate)
VALUES 
-- John Doe (EmployeeID 1) is also assigned to Project 2 and Project 5
(1, 2, 'Developer', '2023-01-01', NULL),
(1, 5, 'Developer', '2023-05-05', NULL),

-- Jane Smith (EmployeeID 2) is assigned to more projects as Project Manager
(2, 1, 'Project Manager', '2023-01-10', '2023-06-30'),
(2, 4, 'Project Manager', '2023-04-01', '2023-10-15'),

-- Michael Brown (EmployeeID 3) working on multiple projects as a Consultant
(3, 1, 'Consultant', '2023-01-15', '2023-06-30'),
(3, 5, 'Consultant', '2023-06-01', NULL),

-- Sarah Johnson (EmployeeID 4) assisting in other projects as Business Analyst
(4, 2, 'Business Analyst', '2022-09-05', NULL),
(4, 5, 'Business Analyst', '2023-05-01', NULL),

-- Emily Davis (EmployeeID 5) working as QA on other projects
(5, 1, 'Quality Assurance', '2023-01-10', '2023-06-30'),
(5, 3, 'Quality Assurance', '2023-03-20', NULL);


-- John Doe (EmployeeID 1) logging more hours on Project 2 and Project 5
INSERT INTO [dbo].[TimeLog] (EmployeeID, ProjectID, LogDate, HoursWorked, Description, DayRate, BilledAmount)
VALUES 
(1, 2, '2023-01-20', 8.00, 'Backend development on ERP system.', 1000, 1000),
(1, 2, '2023-01-21', 6.50, 'Debugging API issues.', 1000, 1000/6.5),
(1, 5, '2023-06-05', 8.00, 'Developed AI integration module.', 1200, 1200),

-- Jane Smith (EmployeeID 2) logging more hours on multiple projects
(2, 1, '2023-01-14', 8.00, 'Project management for website redesign.', 1100, 1100),
(2, 4, '2023-04-03', 7.50, 'Oversaw cybersecurity audit.', 1300, 1300/7.5),
(2, 2, '2023-09-15', 6.00, 'Meeting with client for ERP status update.', 1100, 1100/6.00),

-- Michael Brown (EmployeeID 3) logging more consulting hours on projects
(3, 1, '2023-01-18', 7.00, 'Consulting on front-end architecture.', 1300, 1300/7.00),
(3, 5, '2023-06-06', 8.00, 'Reviewed AI integration plans.', 1300, 1300),

-- Sarah Johnson (EmployeeID 4) logging more business analysis hours on projects
(4, 2, '2022-09-10', 6.50, 'Requirements gathering for ERP implementation.', 1000, 1000/6.50),
(4, 5, '2023-05-05', 7.00, 'Analyzed business needs for AI integration.', 1000, 1000/7.0),

-- Emily Davis (EmployeeID 5) logging more QA hours on different projects
(5, 1, '2023-01-22', 8.00, 'Tested new website features.', 1200, 1200),
(5, 3, '2023-03-25', 6.50, 'Mobile app UI testing.', 1200, 1200/6.5);
