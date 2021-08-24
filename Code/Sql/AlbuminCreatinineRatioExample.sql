SELECT 
    ACU.URXUMS AS AlbuminMgPerL,
    ACU.URXUCR * 10 AS CreatinineMgPerL,
    ACU.URXUMS / (ACU.URXUCR * 10) AS AlbuminCreatinineRatio,
    D.RIAGENDR AS Gender,
    D.RIDAGEMN AS AgeMonths,
    CAST(D.RIDRETH1 AS varchar(1)) AS RaceEthnicity,
    -1 * (CAST(B.DIQ010 AS int) - 2) AS DiabetesStatus
FROM 
    [NhanesLandingZone].[dbo].[AlbuminAndCreatinineUrine] ACU
    INNER JOIN [NhanesLandingZone].[dbo].[DemographicVariablesAndSampleWeights] D ON
        ACU.SEQN = D.SEQN
    INNER JOIN [NhanesLandingZone].[dbo].[Diabetes] B ON
        B.SEQN = D.SEQN
WHERE 
    ACU.URXUMS IS NOT NULL
    AND ACU.URXUCR IS NOT NULL
    AND D.RIAGENDR IS NOT NULL
    AND D.RIDAGEMN IS NOT NULL
    AND D.RIDRETH1 IS NOT NULL
    AND (B.DIQ010 = '1' OR B.DIQ010 = '2')