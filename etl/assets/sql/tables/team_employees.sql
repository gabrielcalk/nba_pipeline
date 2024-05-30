CREATE TABLE team_employees (
    id INT PRIMARY KEY,
    "teamId" INT,
    "isCurrentPlayer" BOOLEAN,
    "fullName" VARCHAR(255),
    position VARCHAR(5),
    height VARCHAR(5),
    weight VARCHAR(10),
    "jerseyNumber" VARCHAR(5),
    college VARCHAR(255),
    country VARCHAR(100),
    "yearsSinceDraft" INT
);
