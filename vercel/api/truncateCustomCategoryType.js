const { Client } = require('pg');

const TABLE = 'hktvmall.hktvmallCustomCategoryType';

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json');

  if (req.method !== 'POST') {
    return res.status(405).json({
      success: false,
      error: 'Method not allowed. Use POST with JSON body.',
      allowedMethod: 'POST'
    });
  }

  // Authorization: require neonDirectApiToken (header: Authorization Bearer <token> or x-api-key)
  const expectedToken = process.env.neonDirectApiToken;
  if (!expectedToken) {
    return res.status(503).json({
      success: false,
      error: 'Authorization not configured (neonDirectApiToken missing).'
    });
  }
  const authHeader = (req.headers.authorization || '').trim();
  const bearerToken = authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : null;
  const apiKey = (req.headers['x-api-key'] || '').trim();
  const providedToken = bearerToken || apiKey || null;
  if (!providedToken || providedToken !== expectedToken) {
    return res.status(401).json({
      success: false,
      error: 'Unauthorized. Provide a valid token in Authorization: Bearer <token> or x-api-key header.'
    });
  }

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();

    const sql = `
      TRUNCATE TABLE ${TABLE}
    `;

    await client.query(sql);
    await client.end();

    return res.status(200).json({
      success: true,
      message: `Table ${TABLE} truncated successfully.`
    });
  } catch (err) {
    try {
      await client.end();
    } catch (_) {}
    console.error('truncateCustomCategoryType error:', err.message);
    return res.status(500).json({
      success: false,
      message: 'Truncate failed.',
      error: err.message
    });
  }
};

