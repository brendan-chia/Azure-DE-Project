CREATE OR ALTER VIEW @{item().name} AS
SELECT *
FROM OPENROWSET(
    BULK 'https://aztutorial.dfs.core.windows.net/gold/SalesLT/@{item().name}/',
    FORMAT = 'DELTA'
) AS [result]

