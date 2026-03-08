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

  let body;
  try {
    body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
  } catch (e) {
    return res.status(400).json({
      success: false,
      error: 'Invalid JSON body.',
      detail: e.message
    });
  }

  const records = Array.isArray(body.records)
    ? body.records
    : Array.isArray(body)
    ? body
    : body && typeof body === 'object'
    ? [body]
    : null;

  if (!records || records.length === 0) {
    // Auth passed and body is valid JSON, but there is nothing to insert.
    return res.status(200).json({
      success: true,
      message: 'No custom category type records to insert. 0 record(s) processed.',
      insertedCount: 0,
      failedCount: 0,
      errors: []
    });
  }

  // Normalize keys to lowercase for case-insensitive access
  const normalizeKeys = (obj) => {
    const normalized = {};
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        normalized[key.toLowerCase()] = obj[key];
      }
    }
    return normalized;
  };

  const normalized = records.map((rec, index) => {
    const lower = normalizeKeys(rec);
    return {
      index,
      categoryCode: lower.categorycode != null ? String(lower.categorycode) : null,
      customType: lower.customtype != null ? String(lower.customtype) : null
    };
  });

  const invalid = normalized.filter((r) => r.categoryCode === null || r.customType === null);
  if (invalid.length > 0) {
    return res.status(400).json({
      success: false,
      error: 'Each record must have categoryCode and customType.',
      invalidIndices: invalid.map((r) => r.index)
    });
  }

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();

    const placeholders = [];
    const values = [];
    let paramIndex = 1;

    for (const row of normalized) {
      placeholders.push(`($${paramIndex}, $${paramIndex + 1})`);
      values.push(row.categoryCode, row.customType);
      paramIndex += 2;
    }

    const sql = `
      INSERT INTO ${TABLE} (categorycode, customtype)
      VALUES ${placeholders.join(', ')}
    `;

    await client.query(sql, values);
    await client.end();

    return res.status(200).json({
      success: true,
      message: `Inserted ${normalized.length} custom category type record(s).`,
      insertedCount: normalized.length,
      failedCount: 0,
      errors: []
    });
  } catch (err) {
    try {
      await client.end();
    } catch (_) {}
    console.error('insertCustomCategoryType error:', err.message);
    return res.status(500).json({
      success: false,
      message: 'Insert failed.',
      error: err.message,
      insertedCount: 0,
      failedCount: normalized.length,
      errors: [{ index: -1, error: err.message }]
    });
  }
};

