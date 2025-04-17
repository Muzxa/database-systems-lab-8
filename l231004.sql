USE SuperDogCarbonDB
GO

-- QUERY 1
WITH EmissionTotals AS (
    SELECT s.site_name, o.organization_name,
           SUM(er.calculated_emission) AS total_emissions
    FROM Emission_Record AS er
    INNER JOIN Site AS s ON er.site_id = s.site_id
    INNER JOIN Organization AS o ON s.organization_id = o.organization_id
    GROUP BY s.site_name, o.organization_name
)
SELECT site_name, organization_name, total_emissions
FROM EmissionTotals

-- QUERY 2
SELECT 
  o.organization_name,
  es.scope_type,
    YEAR(er.record_date) AS emission_year,
    SUM(er.calculated_emission) AS yearly_emission,
    SUM(SUM(er.calculated_emission)) OVER (
        PARTITION BY o.organization_name, es.scope_type 
        ORDER BY YEAR(er.record_date)
    ) AS cumulative_emission
FROM Emission_Record er
INNER JOIN Site AS s ON er.site_id = s.site_id
INNER JOIN Organization AS o ON s.organization_id = o.organization_id
INNER JOIN Emission_Scope AS es ON er.scope_id = es.scope_id
GROUP BY o.organization_name, es.scope_type, YEAR(er.record_date);

-- QUERY 3
SELECT 
    o.organization_name,
    et.year,
    es.scope_type,
    et.emission_limit,
    SUM(er.calculated_emission) AS actual_emission,
    CASE 
        WHEN SUM(er.calculated_emission) > et.emission_limit THEN 'EXCEEDED LIMIT'
        ELSE 'BELOW LIMIT'
    END AS status
FROM Emission_Target et
INNER JOIN Organization AS o ON et.organization_id = o.organization_id
INNER JOIN Emission_Scope AS es ON et.scope_id = es.scope_id
INNER JOIN Site AS s ON s.organization_id = o.organization_id
INNER JOIN Emission_Record AS er 
    ON er.site_id = s.site_id 
    AND er.scope_id = et.scope_id 
    AND YEAR(er.record_date) = et.year
GROUP BY 
    o.organization_name, 
    et.year, 
    es.scope_type, 
    et.emission_limit;

-- QUERY 4
SELECT 
    s.site_name,
    STUFF((
        SELECT DISTINCT ', ' + es2.source_type
        FROM Emission_Record er2
        INNER JOIN Emission_Source AS es2 ON er2.source_id = es2.source_id
        WHERE er2.site_id = s.site_id
        FOR XML PATH(''), TYPE -- I HAD TO LOOK THIS UP
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS emission_sources
FROM Site AS s
WHERE EXISTS (
    SELECT 1 FROM Emission_Record AS er WHERE er.site_id = s.site_id
)
ORDER BY s.site_name;

-- QUERY 5
SELECT 
    s.site_name,
    er.record_date,
    er.calculated_emission,
    LAG(er.calculated_emission) OVER (
        PARTITION BY s.site_id ORDER BY er.record_date
    ) AS previous_emission,
    er.calculated_emission - 
    LAG(er.calculated_emission) OVER (
        PARTITION BY s.site_id ORDER BY er.record_date
    ) AS emission_change
FROM Emission_Record AS er
INNER JOIN Site AS s ON er.site_id = s.site_id;

-- QUERY 6
SELECT
    s.site_name,
    FIRST_VALUE(er.record_date) OVER (PARTITION BY s.site_id ORDER BY er.record_date ASC) AS first_record_date,
    FIRST_VALUE(er.calculated_emission) OVER (PARTITION BY s.site_id ORDER BY er.record_date ASC) AS first_emission
FROM Emission_Record er
INNER JOIN Site AS s ON er.site_id = s.site_id;