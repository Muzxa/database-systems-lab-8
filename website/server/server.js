const express = require('express');
const sql = require('mssql');
const cors = require('cors');
const bodyParser = require('body-parser');

const app = express();
app.use(cors({ origin: 'http://localhost:3000' }));
app.use(bodyParser.json());

// SQL Server config
const config = {
    user: 'SA',
    password: 'Password123',
    server: 'localhost', // or your SQL Server host
    database: 'SuperDogCarbonDB',
    options: {
        encrypt: false,
        trustServerCertificate: true // for local dev
    }
};

// PORT = 8000
// CONFIG = '{"user":"SA","password":"Password123","server":"localhost","database":"HOSPITAL","options":{"encrypt":false,"trustServerCertificate":true},"port":1433}'
// JWT_SECRET = "sehat-app"
// NODE_ENV = "development"

// 1. Add Organization
app.post('/organizations', async (req, res) => {
    const { organization_name, industry_type } = req.body;
    try {
        await sql.connect(config);
        const result = await sql.query`
            INSERT INTO Organization (organization_name, industry_type)
            VALUES (${organization_name}, ${industry_type});
        `;
        res.status(201).json({ message: 'Organization added.' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 2. Get Organizations
app.get('/organizations', async (req, res) => {
    try {
        await sql.connect(config);
        const result = await sql.query`SELECT * FROM Organization`;
        res.json(result.recordset);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 3. Get Pollution Data
app.get('/pollution/:org', async (req, res) => {
    try {
        const orgName = req.params.org;
        const pool = await sql.connect(config);
        const result = await pool
        .request()
        .input('org_name', sql.VarChar(100), orgName)
        .query(`
            SELECT 
                es.source_type,
                SUM(er.calculated_emission) AS total_emission
            FROM Emission_Record er
            INNER JOIN Emission_Source es ON er.source_id = es.source_id
            INNER JOIN Organization o ON er.site_id = o.organization_id
            WHERE 
                YEAR(er.record_date) <= YEAR(DATEADD(MONTH, -1, GETDATE()))
                AND MONTH(er.record_date) <= MONTH(DATEADD(MONTH, -1, GETDATE()))
                AND organization_name = @org_name
            GROUP BY 
                es.source_type
            ORDER BY 
                total_emission DESC;
        `);
        res.json(result.recordset);
    } catch (err) {
        console.log(err);
        res.status(500).json({ error: err.message });
    }
});

const PORT = 8000;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});